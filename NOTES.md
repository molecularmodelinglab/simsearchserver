# Findings

161,408,959 Enamine REAL compounds plus 233,244 supermutants, warm, format v1.
Recall is the depth-1 GED relative appearing in the top 5 after re-ranking.
Latency is retrieval only unless stated. The supermutants are in the scoring
indexes so their relatives are retrievable, and out of a shipping index.

**Shipping: salsa16 to PCA-10, retrieve k=1024, atom pairs 512-bit
(`countSimulation=False`, Tanimoto), return 8. fp32, page 16384.**
98.0% depth-1, 55.8% depth-2, 7.76 ms.

## Retrieve wide, rank structurally

The embedding retrieves well and ranks badly. Ranking by embedding distance
gives 21.3% at every k; re-ranking the retrieved set tracks the ceiling within
a point, at every k and dimension measured.

**Raising k beats raising dimension.** PCA-10 at k=1024 scores 98.0% in 7.8 ms;
PCA-12 at k=128 scores 91.5% in 2.9 ms; native salsa16 at k=128 needs 29.9 ms
for 94.3%.

## Dimension

Latency columns are the current engine; recall is unaffected by it.

| config | k=128 | k=1024 | lat k=128 | lat k=1024 |
|---|---|---|---|---|
| salsa8 native | 24.6% | 71.1% | — | — |
| **PCA-10 of salsa16** | 82.7% | **98.0%** | 2.1 ms | **7.8 ms** |
| PCA-12 of salsa16 | 91.5% | 98.3% | 2.9 ms | 9.3 ms |
| salsa16 native | 94.3% | 98.7% | 29.9 ms | 80.8 ms |
| PCA-16 of salsa24 | 96.0% | 98.5% | 32.0 ms | 102.7 ms |
| salsa24 native | 97.4% | 98.4% | 56.0 ms | 195.9 ms |

- Depth-2 is where the dimensions separate: 55.8% at PCA-10 against 58.7% at
  PCA-12, both at k=1024, where depth-1 differs by 0.3.

- **Recall tracks retained variance, not source-model quality**, across
  dimensions. It does *not* discriminate between bases at a fixed dimension:
  refitting the PCA on a stratified sample raised retention from 98.4% to
  99.1% at d=12 and moved recall by less than the sampling error. Breaks below
  ~78% retention, where absolute dimension binds.
- The corpus is **sorted by molecular size** (17 heavy atoms in shard 0, 35 in
  shard 80), so a fit sampled from a prefix sees one band. That cost 30 points
  of retained variance at d=8 and nothing measurable at d>=10.
- **A bigger source compresses worse at a given d**, spreading information
  wider: salsa16 collapses after component 9, salsa24 after 12, so salsa24 to
  10 keeps only 89.6% and scores 61.0%.
- **At matched d, compression beats training**: PCA-16 of salsa24 beats native
  salsa16 by 1.7 points at k=128. A trained salsa10 would most likely lose to
  PCA-10 of salsa16.
- Effective dimensionality drives cost: at equal d the richer space is slower.

## Re-ranking fingerprint

Swept 13 fingerprints x 5 metrics. **Atom pairs, 512 bits, Tanimoto.**

- Atom pairs beat Morgan: +1.2 depth-1, +5.3 depth-2 on salsa16.
- Width is irrelevant: 512 bits equals 4096, and is 60 GB at 1B.
- Morgan radius 2 beats 3 and 4; larger over-specifies. Fusing the two is worse
  than atom pairs alone, and non-uniform bit allocation loses to uniform.
- Tanimoto over query-biased Tversky: equal at k=128, +7 at k=4096. Tanimoto
  and Dice are order-equivalent by construction.
- `countSimulation=False` generates 23% faster at equal recall.

## Quantization is not worth it at this density

Post-hoc scalar quantization of PCA-10, query in fp32: int8 costs 9.5 points at
k=128 (83.3% to 73.8%), int4 costs 18. **Precision must beat the spacing of
competitors, not the scale of the space.** Consecutive ranks near 128 differ by
~3e-4 while int8 displaces a vector by 4.3e-3, fifteen ranks' worth; int12 is
~1x the gap and int16 0.06x, so **int16 should be lossless and int8 is not**.
Bits needed grow with library density. It also costs speed: int8 visits 32,056
pages against fp32's 3,426, since a displaced query fails more bounds tests.

## Where query time goes

PCA-10, k=1024, in-tree 5.05 ms: scan 65%, page fetch 13.5%, node descent 13%,
frontier heap 8.5%. Of the scan, the bounded max-heap insert is 7.7%.

A **page fetch costs ~355 ns at every page size from 2048 to 32768**, so it is
address translation, not reading: a 14 GB mapping needs ~3.5M page-table
entries and the query path touches unrelated addresses. A record scan costs
8 ns. **One fetch is worth forty scans**, which is what sets the page size.

Re-ranking is 30 ms at k=1024, dominated by SMILES parsing; a stored
fingerprint column removes nearly all of it and takes RDKit out of the query
path.

AVX-512 changes nothing (`-C target-cpu=native`: 7.68 ms either way). The scan
runs at ~7 GB/s, near single-core memory bandwidth, so struct-of-arrays cannot
help: it moves the same bytes.

## Index structure

- **Cold is 600x warm** (4,575 ms vs 7.6 ms at k=1024). Startup prefault takes
  53 s for 8.9 GB, but only with `MADV_SEQUENTIAL` first; against the
  `MADV_RANDOM` used for querying it is 7x slower.
- **The frontier bound must accumulate per axis.** Priority by distance to the
  last split plane alone visits 8,233 pages at k=128 and discards 6,449 of
  them; the distance to the whole cell visits 1,815 and discards 116. Worth
  **2.0x at k=128 and 1.8x at k=1024**, with no format change.
- **A bounded max-heap for the top k.** The sorted-vector insert was 3.9 ms of
  14 ms at k=1024; the heap is 0.4 ms.
- **Page bounding boxes** mattered under the loose frontier bound (66% of
  visits skipped at dim 8). With the accumulated bound only 6% are left to
  reject.
- **Page size 16384 is optimal**, re-confirmed after the bound change: smaller
  pages cut records scanned 9x but lose on fetches and a deeper tree (7.4M
  nodes at 2048 against 863k at 16384).
- **Node-level bounding boxes are not worth it.** They exist to avoid touching
  pages, and the accumulated bound already removed those touches.
- **Splitting on the widest axis** replaced cycling by depth: slightly faster,
  identical recall, and it is what makes duplicate-heavy data buildable.

## Serving

- **Rank before fetching SMILES.** Fetching a `db.db` entry for all k hits to
  serve 8 cost 2.6 ms of a 7.1 ms query at k=1024. Ranking on the fingerprint
  column first and fetching only what is served drops database lookups from
  1,024 to 8.
- Removing the descriptor clone from the heap and the record materialization
  from the scan took in-tree time from 5.05 to 3.93 ms on its own; only the
  8-byte record index is needed while scanning.
- Together, **k=1024 went from 7.76 ms to 4.46 ms (43%)** and k=128 from 2.14
  to 1.74 ms.

| k | served | wall | in-tree | rank |
|---|---|---|---|---|
| 128 | 8 | 1.74 ms | 1.50 ms | 36 us |
| 128 | 128 | 2.07 ms | 1.49 ms | 36 us |
| 1024 | 8 | **4.46 ms** | 3.93 ms | 263 us |
| 1024 | 1024 | 7.10 ms | 3.93 ms | 268 us |

- **Concurrency scales**: at k=1024, 139 qps on one client to 2,975 on 32
  (21.3x on 32 cores), median 6.8 to 9.4 ms, p99 13.8 to 30.5 ms, no errors.
  Nothing in the query path serializes. The k=128 plateau at ~5,500 qps is
  likely the GIL-bound Python client, not the server.
- **Everything the query path touches must be prefaulted, `db.db` included.**
  Warming only `record` and `fingerprints` left the first query into a cold
  region of the SMILES database at **2,396 ms** against 8 ms in-tree: one disk
  read per hit, 1,024 of them. Warmed, the same queries take 6-21 ms.
- Prefaulting all 35.5 GB takes **12 s** on an uncontended `/work`; a 377 s
  reading on the same files was NFS contention, not a real cost.
- Re-ranking 1,024 candidates against the stored fingerprints costs 0.3 ms,
  against ~30 ms when it parsed SMILES per query.

## On the production host, measured

7950X, 128 GB, local NVMe, one GPU, 161M index.

| | |
|---|---|
| embed (GPU, one molecule) | 12.9 ms |
| project + fingerprint | 0.17 ms |
| index, warm | 3.0 ms |
| **end to end, warm** | **17.8 ms** |

- **Embedding is 72% of a query and a GPU barely helps**: 35 ms on CPU against
  12.9 on GPU, because one molecule is a batch of one and the cost is kernel
  launch, not compute. More GPU will not move it; `torch.compile`, CUDA graphs
  or fp16 might.
- **Cold is 168x warm** (502 ms against 3.0 ms at k=1024), decaying to 72x by
  the fourth query as the page cache fills. An earlier estimate of ~30x was
  wrong by five-fold. `MADV_RANDOM` is right for a warm index and wrong for a
  cold one, since it disables readahead.
- **The service saturates at ~152 qps**, flat from 4 clients up while latency
  grows linearly (26 ms at 4 clients, 104 ms at 16). That ceiling implies a
  **6.6 ms serialized section**, which is the model lock around inference.
- **Batch-1 inference wastes the GPU.** On an L40S, batch 32 costs 14.7 ms
  against 8.3 ms for batch 1 — 1.8x the time for 32x the molecules, **18x the
  throughput**; batch 128 reaches 26x. Dynamic batching is therefore the lever
  for concurrency, not more workers and not MPS: the GPU is underused, not
  contended, so sharing it better buys nothing.

| batch | ms | mol/s |
|---|---|---|
| 1 | 8.3 | 120 |
| 32 | 14.7 | 2,173 |
| 128 | 40.9 | 3,134 |

## Bulk embedding peaks at batch 64

Throughput is not monotonic in batch size, because every batch pads to its
longest SMILES: past some point the padding costs more than the kernel launches
it saves. Measured on an L40S over 40,000 REAL molecules (`scripts/batch_probe.py`):

| batch | mol/s |
|---|---|
| 32 | 2,278 |
| 48 | 2,665 |
| **64** | **2,791** |
| 96 | 2,761 |
| 128 | 2,483 |
| 256 | 2,683 |

The 161M corpus was embedded at batch 1024 and averaged 2,700 mol/s, so the
peak is worth about 10%. Sorting each shard by SMILES length would remove the
padding waste altogether and let large batches win again -- untried.

## Metrics that did not work

MCS and Tanimoto averaged over the top 5 do not separate models at this scale
(1.19x and 1.28x against 2.04x for GED recall): they regress toward the
library's background similarity, which at 161M is high.

## Open model-side question

Re-ranking recovers ~98% of the depth-1 ceiling but only ~62% of depth-2 in
every model. SALSA's contrastive training uses only 1-edit pairs, so the loss
never constrains 2-edit ordering. Higher dimensions keep improving depth-2
(54.1% at d=10, 60.3% at d=16), which suggests multi-GED training data pays.

## Measurement discipline

- **Cheap probes mispredicted the in-situ result five times**: depth-ordering
  correlation on salsa24, quantization distance error, top-10 overlap in a
  200k pool, AVX-512 width, and retained variance across bases. Triage only.
- **Microbenchmarks overstated gains 1.75-2.5x, three times.**
- **Warm with the exact queries to be timed**, not a fixed prefix.
- **Fixing the dominant cost promotes the next one.** The scan was 31% of query
  time and dismissed; after the frontier bound it was 65% and unchanged in
  absolute terms. Shares are not conclusions.
- **Check a new measurement against a known one.** A 57 ms reading against a
  known 8.1 ms exposed a wrong query projection, not a slow index.
- **Run the tests in debug too.** Release wrapped a `len() - 1` underflow to
  `usize::MAX` and wrapped it back on read; debug caught it.
