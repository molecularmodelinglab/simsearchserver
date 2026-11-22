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

## Install

Rust (stable; the build no longer needs nightly):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cd rust_server && cargo build --release
```

Python:

```bash
python -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
```

Model weights download and cache automatically on first use.

## Building an index

Input is parquet (preferred) or CSV with an `ID` column, a `SMILES` column, and
one `dim_<i>` column per embedding dimension — the layout `salsa-lit embed`
writes:

```bash
salsa-lit embed molecules.parquet -m salsa16 -o embeddings.parquet
```

Then build. `desc_length` must match the embedding dimension:

```bash
cat > config.yaml <<EOF
directory: "/path/to/index"
desc_length: 16
record_page_length: 16384
node_page_length: 8192
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
| `record` | leaf pages of `(u64 index, desc_length x f32)` records |
| `db.db` | fixed-width rows of 8 unused + 200 SMILES + 50 identifier bytes, addressed by the record index |

`node_page_length` is accepted in the config but never read; only
`record_page_length` affects the index.

## Running

Development:

```bash
# embedding service
cd salsa_server && FLASK_APP=main.py ../.venv/bin/python -m flask run --port 5000

# kd-tree service
./rust_server/target/release/server --dirname /path/to/index --port 3000

# web UI
FLASK_APP=python_web_server/main.py ./.venv/bin/python -m flask run --port 8000
```

Production:

```bash
cd salsa_server && gunicorn --workers 1 --threads 4 --bind 127.0.0.1:5000 wsgi:app
gunicorn --workers 4 --bind 127.0.0.1:8000 python_web_server.wsgi:app
./rust_server/target/release/server --dirname /path/to/index --port 3000
```

Each `salsa_server` worker loads its own copy of the model, so prefer threads
over workers there.

### Configuration

| Variable | Default | Used by |
|---|---|---|
| `SALSA_MODEL` | `salsa16` | `salsa_server` — a registry name (`salsa8`/`salsa16`/`salsa24`/`salsa32`) or a path |
| `SALSA_URL` | `http://127.0.0.1:5000` | `python_web_server` |
| `KDTREE_URL` | `http://127.0.0.1:3000` | `python_web_server` |
| `NUM_NEIGHBORS` | `100` | `python_web_server` |
| `REQUEST_TIMEOUT` | `30` | `python_web_server` |
| `HF_HOME` | — | model download cache |

The kd-tree server binds loopback by default; pass `--host 0.0.0.0` to accept
off-host connections.

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

## Tests and benchmarks

```bash
cd rust_server
python3 scripts/generate_test_fixtures.py   # regenerate gitignored test data
cargo test --release
```

The accuracy tests check `get_nearest_neighbors` against an exhaustive scan.

```bash
# recall and latency for a built index
python3 rust_server/scripts/benchmark_index.py \
    --index /path/to/index --source 'data/embeddings/*.parquet' --desc-length 16

# sweep record_page_length
python3 rust_server/scripts/sweep_page_size.py \
    --filenames data/embeddings/enamine_0000.parquet --desc-length 16 --workdir /tmp/sweep
```
