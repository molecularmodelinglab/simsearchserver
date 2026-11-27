# Setting up a server

Three stages: install, prepare a library and build its index, then run the two
services. If you already have an index, skip to [Running](#running).

## Install

```bash
git clone https://github.com/molecularmodelinglab/simsearchserver
cd simsearchserver

python -m venv .venv && source .venv/bin/activate
pip install -e .
```

Rust, via [rustup](https://rustup.rs) rather than a distribution package —
`rust-toolchain.toml` pins the channel and only rustup reads it:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable

cd rust_server
cargo build --release
cd ..
```

On a known production host, name its microarchitecture rather than using
`native`, which bakes in whatever CPU happened to compile it:

```bash
cargo build --release --config 'build.rustflags=["-C","target-cpu=znver4"]'
```

Model weights download on first use. If `~/.cache/huggingface` is not writable,
set `embedding.cache_dir` in the service config rather than fighting it.

## Prepare a library

One config drives every stage, so they cannot disagree about which model or
dimension was used:

```yaml
# prepare.yaml
output: data/prepared
corpus:      {input: data/smiles/, id_column: ID, smiles_column: SMILES}
embedding:   {model: salsa16, revision: null, batch_size: 1024}
projection:  {method: pca, dim: 10, fit_rows: 4000000}
fingerprint: {type: atompair, bits: 512, count_simulation: false}
index:       {directory: data/index/prod, record_page_length: 16384}
```

Input is parquet or CSV with an `ID` and a `SMILES` column.

```bash
simsearch prepare prepare.yaml
```

Stages skip work already done, so a partial run can be resubmitted. Each stage
also runs alone (`simsearch embed|project|fingerprint`), and
`--shard i --of n` splits the work for a job array.

`projection: {method: none}` is supported, for a model whose output dimension
you want to index directly.

`prepare` ends by writing `index.yaml` and printing the builder command for it.

## Build the index

```bash
rust_server/target/release/builder build-from-files \
    --filenames 'data/prepared/projection/*.parquet' \
    --config-filename data/prepared/index.yaml \
    --fingerprints data/prepared/fingerprint
```

The directory must not already exist. Records that cannot be stored — an
over-length or non-ASCII SMILES or identifier — are logged to `build_log.txt`
inside the index and skipped; the build does not abort. The builder refuses to
start if a file's `dim_*` column count disagrees with `desc_length`, rather
than indexing truncated vectors.

`--id-prefix` prepends a string to every identifier, which keeps records from
different datasets distinguishable in one index. It defaults to empty, so
identifiers are yours untouched.

Then record what the index was built from:

```bash
simsearch finalize prepare.yaml
```

This writes `config.resolved.yaml` and `transform.json` into the index: the
model and revision, the record count, the fingerprint spec, and a hash of the
PCA basis. The server validates against the first and serves the second, so a
query can never be projected with a basis the index was not built on.

### What an index contains

| File | Contents |
|---|---|
| `config.yaml` | dimension, page size, format version, build date |
| `config.resolved.yaml` | what it was built from, written by `finalize` |
| `transform.json` | the projection, served at `/transform` |
| `node` | internal nodes: split axis, split value, child pointers |
| `record` | leaf pages: a per-axis bounding box, then the records |
| `db.db`, `db.db.idx` | identifiers and SMILES, addressed by record index |
| `fingerprints` | one fixed-width fingerprint per record |

An index is relocatable: `--dirname` wins over the path recorded inside it.

## Running

Two processes.

### The index

```bash
rust_server/target/release/server --dirname /path/to/index --port 3000
```

It prefaults the index at startup — about 12 s for 36 GB on local NVMe — then
validates against `config.resolved.yaml` and refuses to serve on a mismatch.

**Keep it on loopback.** It has no authentication and a query costs real CPU.

`--batch-threads` sets how many threads a batch query may use, defaulting to
all but two cores so a batch cannot starve interactive queries.

### The query service

Copy `simsearch_api/service.yaml.example` and edit it:

```yaml
embedding:
  model: salsa16
  device: cuda          # null for CPU, which costs ~3x on embedding
  cache_dir: null       # set this if the default HF cache is not writable

cache:   {ttl_seconds: 21600, max_entries: 5000}
limits:
  max_smiles_length: 150
  max_num_neighbors: 96
  max_batch_size: 10000     # POST /api/search/batch
  batch_concurrency: 1      # a second batch gets 429, not a queue
  batch_timeout_seconds: 300.0

static_dir: /absolute/path/to/web/dist

libraries:
  - name: real161m
    display: "Enamine REAL (161M)"
    description: "161,408,959 make-on-demand compounds"
    url: http://127.0.0.1:3000
    max_neighbors: 1024   # candidates retrieved, the accuracy dial
    num_neighbors: 8      # served, when a caller does not say
```

```bash
SIMSEARCH_API_CONFIG=service.yaml python -m uvicorn simsearch_api.app:app --port 8000
```

Start the index first: the service reads each library's `/health` and
`/transform` at startup and will not start without them.

Adding a library is an edit here and a restart — one `server` process per
library, each with its own index and projection.

### The frontend

```bash
cd web && npm install && npm run build
```

With `static_dir` set, `http://localhost:8000/` serves the whole application.
During development `npm run dev` proxies `/api` to port 8000.

## Checking it

```bash
curl localhost:8000/api/health      # model loaded, libraries reachable
curl localhost:8000/api/libraries   # record counts and transform digests
curl -G localhost:8000/api/search --data-urlencode 'smiles=CC#N' -d n=8
curl -X POST localhost:8000/api/search/batch -H 'Content-Type: application/json' \
     -d '{"smiles": ["CC#N", "c1ccccc1O"]}'
```

Every search returns a `timing` block. On a 7950X with a GPU and a 161M index,
expect roughly 13 ms to embed, 3 ms in the index and 18 ms end to end.

## Going public

The query service is the only thing a browser should reach. Put nginx in front
for TLS, static assets and rate limiting, and leave `rust_server` on loopback.

Limit **concurrency**, not only request rate: the service saturates near
152 queries per second, and past about 4 concurrent requests latency grows
linearly while throughput does not.

## Tests

```bash
cd rust_server
python3 scripts/generate_test_fixtures.py   # gitignored test data
cargo test
cd .. && python -m pytest tests/ -q
```

The serving tests build a small index, serve it and query it; they skip unless
the Rust binaries are built.
