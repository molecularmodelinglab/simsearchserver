# Findings

Measured on 161,642,203 Enamine REAL compounds, warm, format v3.
Ground truth is a supermutant set: chains of five consecutive single graph
edits, so recall at edit depth *d* is "did the *d*-edit relative come back".

## The embedding retrieves well and ranks poorly

The depth-1 relative is in the **top 5 by embedding distance 21%** of the time,
but is **somewhere in the retrieved 1024 72%** of the time. Re-ranking those
candidates structurally recovers almost all of it.

| retrieve k | ceiling | after re-rank | latency |
|---|---|---|---|
| 128 | 48.9% | 48.7% | 2.1 ms |
| 512 | 62.9% | 62.5% | 5.8 ms |
| **1024** | **71.6%** | **71.1%** | **7.5 ms** |
| 4096 | 87.6% | 86.7% | 81.5 ms |

Ranking by distance gives 21% at every k — retrieving more never helps on its
own. **Retrieve wide, rank structurally, show few.**

## k beats dimension

| | salsa8 | salsa16 |
|---|---|---|
| k=8 | 0.29 ms | 10.45 ms |
| k=128 | 1.12 ms | 38.41 ms |
| k=1024 | 7.46 ms | 87.50 ms |
| depth-1 after re-rank, k=1024 | 71% | ~93% |

Raising k is far cheaper than raising dimension. The gap narrows with k (36x at
k=8, 12x at k=1024) because salsa8's cost grows 26x from k=8 to k=1024 while
salsa16's grows 8.4x. salsa8 at k=1024 ships.

## Fingerprint and metric for re-ranking

Swept 13 fingerprints x 5 metrics on fixed candidate sets.

- **Atom pairs beat Morgan**, replicated on both models: +1.2 points at depth 1
  and +5.3 at depth 2 on salsa16.
- **Width does not matter**: 512 bits equals 4096. Clashes disturb similarity
  values but not the order of the top few. 512 bits is 60 GB at 1B.
- **Morgan radius 2 is the optimum**, not 3 or 4. Larger radius over-specifies
  and near-analogs stop sharing bits.
- **Tanimoto**, not query-biased Tversky: identical at k=128, but Tversky loses
  7 points at k=4096, where "covers the query" admits large molecules that
  contain the query and much else. Tanimoto and Dice are order-equivalent by
  construction, so they can never rank differently.
- Fusing Morgan with atom pairs is **worse** than atom pairs alone.

## Where query time goes

At k=1024: 7.8 ms retrieve, 30 ms re-rank, 38 ms end to end. Re-ranking is
dominated by SMILES parsing, which is why it runs on RDKit's thread pool
(`MultithreadedSmilesMolSupplier`, 6.2x over `MolFromSmiles` in a loop).

Both parsing and fingerprinting recompute per query something fixed at build
time; a stored fingerprint column would remove ~29 of those 38 ms.

## Cold index is 600x slower

4,575 ms per query cold against 7.6 ms warm. The server faults the record file
in at startup (53 s for 8.9 GB). This needs `MADV_SEQUENTIAL` first — with the
`MADV_RANDOM` used for querying, the prefault is 7x slower.

## Page bounding boxes

A per-axis box in each leaf page header skips pages without reading their
records: **66% of visited pages at dim 8, 91% at dim 16**, worth about 1.4x.
It did not narrow the gap between the two models — both gained ~1.6x.

## Metrics that did not work

MCS and Tanimoto averaged over the top 5 **do not discriminate between models**
at this scale (1.19x and 1.28x, against 2.04x for GED recall). They average
over all hits and regress toward the library's background similarity, which at
161M is high: even a mediocre embedding returns molecules sharing half their
atoms. GED recall is a threshold test against one target, so it amplifies.

## The depth-2 cliff is probably a training artifact

Re-ranking recovers ~98% of the depth-1 ceiling but only ~62% of depth-2, in
both models. SALSA's contrastive training uses only 1-edit pairs, so the loss
never constrains the ordering of 2-edit and 3-edit neighbors. Open question on
the model side.

## Measurement discipline

Two mistakes made repeatedly, both easy to repeat:

1. **Microbenchmarks overstated in-situ gains by 1.75x-2.5x, three times.**
   Treat them as hypotheses; confirm in the server.
2. **Cold vs warm page cache dominates everything.** Warm with the exact
   queries that will be timed, not a fixed prefix.

Also: one cluster node was silently clock-throttled to 500 MHz against 4100 on
the others, making every timing on it meaningless. Check the machine before
believing a regression.
