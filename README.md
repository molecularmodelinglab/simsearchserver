# simsearchserver

Fast chemical nearest-neighbor search over low-dimensional molecular
embeddings, backed by a disk-resident kd-tree. Implements the approach in
[arXiv:2402.07970](https://arxiv.org/abs/2402.07970).

## Architecture

Three processes. The kd-tree server knows nothing about chemistry — it searches
vectors. All chemistry (tokenization, canonicalization, embedding) lives in
[SALSA](https://github.com/molecularmodelinglab/SALSA).

```
browser ──► python_web_server ──► salsa_server    (SMILES  -> embedding)
             (Flask UI)       └─► rust_server     (vector  -> neighbors)
```

| Component | What it does |
|---|---|
| `rust_server/kd_tree` | the index: disk-paged kd-tree library |
| `rust_server/builder` | builds an index from parquet or CSV embedding files |
| `rust_server/server` | HTTP vector search over a built index |
| `salsa_server` | embeds a SMILES string via the SALSA model |
| `python_web_server` | Flask UI; orchestrates the two services above |

The web UI embeds its query first, then sends the **vector** to the kd-tree
server. The kd-tree server never calls out to the embedding service.

Retrieval and ranking are separate: the index returns `MAX_NEIGHBORS`
candidates, and `python_web_server` re-ranks them by atom-pair Tanimoto
similarity before showing `NUM_NEIGHBORS`. The embedding retrieves well and
ranks poorly, so this is worth roughly 3x in retrieval accuracy.

## Install

Rust (stable; the build no longer needs nightly):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cd rust_server && cargo build --release
```

Python:

```bash
python -m venv .venv
./.venv/bin/python -m pip install -e .
```

Model weights download and cache automatically on first use.

For a production host, name its microarchitecture rather than using `native`,
which bakes in whatever CPU happened to build it:

```bash
cargo build --release --config 'build.rustflags=["-C","target-cpu=znver4"]'
```

This is not the default because such a binary faults on an older CPU, and it
is not faster at the dimensions served here — the scan walks one record at a
time, so the vector unit goes unused either way.

## Preparing a library

One config drives embedding, projection and fingerprinting, so the stages
cannot disagree about which model or dimension was used:

```yaml
output: data/prepared
corpus:      {input: data/smiles/}
embedding:   {model: salsa16}
projection:  {method: pca, dim: 10}     # method: none skips it
fingerprint: {type: atompair, bits: 512}
index:       {directory: data/index/prod}
```

```bash
simsearch prepare config.yaml            # embed, project, fingerprint
simsearch project config.yaml            # or one stage at a time
simsearch embed config.yaml --shard 3 --of 27   # for a slurm array
```

Stages skip work already done, so a partial run can be resubmitted. `prepare`
ends by writing `index.yaml` and printing the builder command for it.

## Building an index

Input is parquet (preferred) or CSV with an `ID` column, a `SMILES` column, and
one `dim_<i>` column per embedding dimension — the layout `salsa-lit embed`
writes:

```bash
salsa-lit embed molecules.parquet -m salsa16 -o embeddings.parquet
```

Then build. `desc_length` must match the embedding dimension, and
`--fingerprints` attaches the prepared fingerprint column:

```bash
cat > config.yaml <<EOF
directory: "/path/to/index"
desc_length: 16
record_page_length: 16384
EOF

./rust_server/target/release/builder build-from-files \
    --filenames 'data/embeddings/*.parquet' \
    --config-filename config.yaml
```

The directory must not already exist. `--limit N` stops after N records, for
timing runs. Records that cannot be stored (over-length or non-ASCII SMILES or
identifiers) are logged to `build_log.txt` in the index directory and skipped;
the build does not abort.

Parquet columns are projected by name, so column order does not matter and
extra columns are ignored. Snappy, zstd, lz4 and uncompressed parquet are
supported. The builder refuses to start if the file's `dim_*` column count
disagrees with `desc_length`, rather than indexing truncated vectors.

`builder` prefixes every identifier with the leading underscore-delimited part
of its source filename, so `enamine_0003.parquet` yields `enamineZ1234`. This
keeps records from different source datasets distinguishable in one index.

On a cluster:

```bash
sbatch submit_build_index.sh /path/to/index 16 'data/embeddings/*.parquet'
```

### Index layout

| File | Contents |
|---|---|
| `config.yaml` | dimension and page sizes |
| `node` | internal nodes: split axis, split value, child pointers. No points. Loaded wholly into memory when serving. |
| `record` | leaf pages: a per-axis bounding box, then `(u64 index, desc_length x f32)` records |
| `db.db` | variable-length identifier and SMILES, addressed by the record index |
| `db.db.idx` | one `u64` offset per record into `db.db` |

`record_page_length` is the only page size that matters; 16384 measured best
at 161M records and the curve is shallow either side of it.

## Running

Development:

All three run from the repository root.

```bash
# embedding service
FLASK_APP=salsa_server.main flask run --port 5000

# kd-tree service
./rust_server/target/release/server --dirname /path/to/index --port 3000

# web UI
FLASK_APP=python_web_server.main flask run --port 8000
```

Production:

```bash
gunicorn --workers 1 --threads 4 --bind 127.0.0.1:5000 salsa_server.wsgi:app
gunicorn --workers 4 --bind 127.0.0.1:8000 python_web_server.wsgi:app
./rust_server/target/release/server --dirname /path/to/index --port 3000
```

The kd-tree server faults the record file into the page cache before it
accepts traffic, which takes about a minute per 10 GB. Skip it with
`--no-warm` only for an index larger than memory: cold, queries are orders of
magnitude slower.

Each `salsa_server` worker loads its own copy of the model, so prefer threads
over workers there.

### Configuration

| Variable | Default | Used by |
|---|---|---|
| `SALSA_MODEL` | `salsa16` | `salsa_server` — a registry name (`salsa8`/`salsa16`/`salsa24`/`salsa32`) or a path |
| `SALSA_URL` | `http://127.0.0.1:5000` | `python_web_server` |
| `KDTREE_URL` | `http://127.0.0.1:3000` | `python_web_server` |
| `MAX_NEIGHBORS` | `1024` | `python_web_server` — candidates retrieved from the index, before re-ranking |
| `NUM_NEIGHBORS` | `8` | `python_web_server` — results returned after re-ranking. A power of two, so a full stereoisomer tie group comes back together |
| `RERANK_FP_BITS` | `512` | `python_web_server` — atom-pair fingerprint width |
| `RERANK_PARSE_THREADS` | all cores | `python_web_server` — lower it when serving concurrent queries |
| `REQUEST_TIMEOUT` | `30` | `python_web_server` |
| `HF_HOME` | — | model download cache |

The kd-tree server binds loopback by default; pass `--host 0.0.0.0` to accept
off-host connections.

## Web service

The query service turns a SMILES into ranked analogs: it embeds with SALSA,
projects with the index's own transform, fingerprints with RDKit and asks the
kd-tree. Libraries are declared in its config, so adding one is an edit:

```yaml
libraries:
  - name: real161m
    display: "Enamine REAL (161M)"
    url: http://127.0.0.1:3000
```

```bash
SIMSEARCH_API_CONFIG=service.yaml \
    python -m uvicorn simsearch_api.app:app --port 8000
```

It refuses to serve a library whose `/transform` is missing, because an
unprojected query is the wrong dimension for the index.

The frontend builds to static files served by the query service or by nginx:

```bash
cd web && npm install && npm run build   # dist/
```

Set `static_dir` to `web/dist` to serve both from one process, or point nginx
at it and proxy `/api`. In development `npm run dev` proxies `/api` to port
8000.

## HTTP API

`salsa_server` — SMILES travels in the body or query string, never the path,
since SMILES contain `/` and `#`:

```bash
curl -X POST localhost:5000/smiles/salsa16 -H 'Content-Type: application/json' \
     -d '{"smiles":"C/C=C/C(=O)Oc1ccccc1"}'
curl -G localhost:5000/smiles/salsa16 --data-urlencode 'smiles=CC#N'
curl localhost:5000/health
```

`rust_server` — a query vector must match the index's `desc_length`:

```bash
curl -X POST localhost:3000/nn/10 -H 'Content-Type: application/json' \
     -d '{"descriptor":[0.1, ...]}'
curl 'localhost:3000/nn/10/0.1,0.2,...'      # convenience form for curl
curl localhost:3000/health
```

Sending the query's fingerprint has the server re-rank the retrieved set
against its stored fingerprints and return `num_neighbors` hits, each with a
`similarity`. This is the serving path: retrieve wide by embedding, rank by
structure, with no RDKit in the request.

```bash
curl -X POST localhost:3000/nn/1024 -H 'Content-Type: application/json' \
     -d '{"descriptor":[...], "fingerprint":[1,0,...], "num_neighbors":8}'
```

`/health` reports `fingerprint_bytes` when a fingerprint file is present. An
index built without one still serves, in embedding order.

## Tests and benchmarks

```bash
cd rust_server
python3 scripts/generate_test_fixtures.py   # regenerate gitignored test data
cargo test --release
```

```bash
python3 tests/test_rerank.py                # or: pytest
```

See `tests/README.md` for the layout. The accuracy tests check
`get_nearest_neighbors` against an exhaustive scan.

```bash
# recall and latency for a built index
python3 rust_server/scripts/benchmark_index.py \
    --index /path/to/index --source 'data/embeddings/*.parquet' --desc-length 16

# sweep record_page_length
python3 rust_server/scripts/sweep_page_size.py \
    --filenames data/embeddings/enamine_0000.parquet --desc-length 16 --workdir /tmp/sweep
```
