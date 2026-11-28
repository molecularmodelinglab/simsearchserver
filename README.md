# SmallSA

Chemical similarity search over billion-scale libraries. Finds low
graph-edit-distance analogs for a query structure in milliseconds, by embedding
molecules as low-dimensional vectors and searching a disk-resident kd-tree.

Implements [arXiv:2402.07970](https://doi.org/10.48550/arXiv.2402.07970), on
top of the SALSA embedding model
([paper](https://doi.org/10.1609/aaai.v38i12.29221),
[code](https://github.com/molecularmodelinglab/SALSA)).

## How it works

A SMILES is embedded by SALSA, projected to the dimension its index was built
at, and the index returns the nearest records by Euclidean distance. Those
candidates are then re-ranked by atom-pair Tanimoto similarity against a
fingerprint column stored beside the index.

The second step is what makes it accurate. The embedding **retrieves** well and
**ranks** poorly, so searching wide and ranking structurally is worth roughly
3x over the embedding order alone.

```
browser ──► simsearch_api ──► SALSA model, in process  (SMILES -> descriptor)
                              └─► rust_server          (descriptor -> neighbors)
```

| Component | What it is |
|---|---|
| `rust_server/kd_tree` | the index: a disk-paged kd-tree library |
| `rust_server/builder` | builds an index from parquet or CSV |
| `rust_server/server` | HTTP vector search over a built index |
| `simsearch` | CLI that prepares a library: embed, project, fingerprint |
| `simsearch_api` | query service: all the chemistry, and the only public surface |
| `web` | React frontend, Ketcher for drawing and RDKit for depiction |

The kd-tree server knows no chemistry. It takes a vector and returns record
indexes, which is why it stays on loopback and needs no Python.

## Setup

**[SETUP.md](SETUP.md)** — installing, preparing a library, building an index,
and running the two services.

## What was measured

**[NOTES.md](NOTES.md)** — the dimension and k sweeps, the fingerprint choice,
why quantization was rejected, and where query time goes.

The shipping configuration is salsa16 projected to 10 dimensions, retrieving
1024 candidates and re-ranking to 8: **98.0% depth-1 GED recall** at **17.8 ms**
end to end over 161 million compounds.

## HTTP API

`simsearch_api` is what a client talks to. SMILES travel in the query string or
body, never the path, since they contain `/` and `#`:

```bash
curl -G localhost:8000/api/search --data-urlencode 'smiles=CC#N' -d n=8
curl localhost:8000/api/libraries
curl localhost:8000/api/health
```

Up to 10,000 structures can be searched in one call:

```bash
curl -X POST localhost:8000/api/search/batch \
     -H 'Content-Type: application/json' \
     -d '{"smiles": ["CC#N", "c1ccccc1O"], "num_neighbors": 8}'

# `format: "csv"` returns one row per hit instead, for a dataframe
curl -X POST localhost:8000/api/search/batch \
     -H 'Content-Type: application/json' \
     -d '{"smiles": ["CC#N"], "format": "csv"}' -o hits.csv
```

`rust_server` takes descriptors, not structures, and must be given a vector
matching the index's dimension:

```bash
curl -X POST localhost:3000/nn/1024 -H 'Content-Type: application/json' \
     -d '{"descriptor":[0.1, ...], "num_neighbors":8}'
curl -X POST localhost:3000/nn/batch/1024 -H 'Content-Type: application/json' \
     -d '{"queries":[{"descriptor":[0.1, ...]}], "num_neighbors":8}'
curl localhost:3000/health
curl localhost:3000/transform
```

The batch form runs its queries across a thread pool, which is where a large
submission gets its speed: the index is read-only, so queries need no
coordination at all.

## Credits

Developed by Enes Kelestemur, Travis Maxfield, James Wellnitz, Josh Hochuli and
Kat Kirchoff at the
[Molecular Modeling Lab](https://molecularmodelinglab.github.io/), UNC.

Structure editing by [Ketcher](https://github.com/epam/ketcher); depiction and
fingerprints by [RDKit](https://www.rdkit.org/).
