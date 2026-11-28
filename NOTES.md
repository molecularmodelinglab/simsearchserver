# Findings

161,408,959 Enamine REAL compounds plus 233,244 supermutants, warm, format v1.
Recall is the depth-1 GED relative in the top 5 after re-ranking; latency is
retrieval only unless stated. Supermutants are in the scoring indexes so their
relatives are retrievable, and out of a shipping index.

**Shipping: salsa16 to PCA-10, retrieve k=1024, atom pairs 512-bit
(`countSimulation=False`, Tanimoto), return 8. fp32, page 16384.**
98.0% depth-1, 55.8% depth-2, 4.46 ms retrieval.

## Dimension and k

Embedding distance alone ranks at 21.3% at every k. Re-ranking the retrieved
set tracks the ceiling within a point at every k and dimension measured.
**Raising k beats raising dimension.** Latencies predate the serving work
below; read them as ratios.

| config | k=128 | k=1024 | lat k=128 | lat k=1024 |
|---|---|---|---|---|
| salsa8 native | 24.6% | 71.1% | — | — |
| **PCA-10 of salsa16** | 82.7% | **98.0%** | 2.1 ms | **7.8 ms** |
| PCA-12 of salsa16 | 91.5% | 98.3% | 2.9 ms | 9.3 ms |
| salsa16 native | 94.3% | 98.7% | 29.9 ms | 80.8 ms |
| PCA-16 of salsa24 | 96.0% | 98.5% | 32.0 ms | 102.7 ms |
| salsa24 native | 97.4% | 98.4% | 56.0 ms | 195.9 ms |

- Depth-2 separates dimensions where depth-1 does not: 55.8% at PCA-10 against
  58.7% at PCA-12, k=1024, where depth-1 differs by 0.3.
- **Recall tracks retained variance, not source-model quality.** Not
  discriminating between bases at fixed d: a stratified refit raised retention
  98.4% to 99.1% at d=12 and moved recall less than sampling error. Breaks
  below ~78% retention.
- Corpus is **sorted by molecular size** (17 heavy atoms in shard 0, 35 in
  shard 80). A prefix-sampled fit costs 30 points of retained variance at d=8,
  nothing at d>=10.
- **A bigger source compresses worse at a given d.** salsa16 collapses after
  component 9, salsa24 after 12; salsa24 to 10 keeps 89.6% and scores 61.0%.
- **At matched d, compression beats training**: PCA-16 of salsa24 beats native
  salsa16 by 1.7 points at k=128.
- At equal d the richer space is slower.

## Re-ranking fingerprint

Swept 13 fingerprints x 5 metrics. **Atom pairs, 512 bits, Tanimoto.**

- Atom pairs beat Morgan: +1.2 depth-1, +5.3 depth-2 on salsa16.
- 512 bits equals 4096. 512 is 60 GB at 1B.
- Morgan radius 2 beats 3 and 4. Fusing with atom pairs is worse than atom
  pairs alone; non-uniform bit allocation loses to uniform.
- Tanimoto over query-biased Tversky: equal at k=128, +7 at k=4096. Tanimoto
  and Dice are order-equivalent by construction.
- `countSimulation=False` generates 23% faster at equal recall.
- Atom pairs are stereo-invariant.

## Quantization is not worth it at this density

PCA-10, query fp32, k=128: int8 costs 9.5 points (83.3% to 73.8%), int4 costs
18. int8 also visits 32,056 pages against fp32's 3,426.

**Precision must beat the spacing of competitors, not the scale of the space.**
Consecutive ranks near 128 differ by ~3e-4; int8 displaces a vector 4.3e-3,
fifteen ranks' worth. int12 ~1x the gap, int16 0.06x, so int16 is lossless.
Bits needed grow with library density.

## Where query time goes

PCA-10, k=1024, in-tree 5.05 ms: scan 65%, page fetch 13.5%, node descent 13%,
frontier heap 8.5%. The max-heap insert is 7.7% of the scan.

- **A page fetch costs ~355 ns at every page size from 2048 to 32768** —
  address translation, not reading; a 14 GB mapping needs ~3.5M page-table
  entries. A record scan costs 8 ns. **One fetch is worth forty scans**, which
  sets the page size.
- Re-ranking was 30 ms at k=1024, dominated by SMILES parsing. The stored
  fingerprint column makes it 0.3 ms and removes RDKit from the query path.
- AVX-512 changes nothing (7.68 ms either way). The scan runs at ~7 GB/s, near
  single-core memory bandwidth, so struct-of-arrays moves the same bytes.

## Index structure

- **The frontier bound must accumulate per axis.** Distance to the last split
  plane visits 8,233 pages at k=128 and discards 6,449; distance to the whole
  cell visits 1,815 and discards 116. **2.0x at k=128, 1.8x at k=1024**, no
  format change.
- **A bounded max-heap for the top k**: sorted-vector insert was 3.9 ms of
  14 ms at k=1024, the heap is 0.4 ms.
- **Page size 16384 is optimal**, re-confirmed after the bound change. Smaller
  pages cut records scanned 9x but lose on fetches and tree depth (7.4M nodes
  at 2048 against 863k at 16384).
- **Page bounding boxes** skipped 66% of visits under the loose bound; 6% under
  the accumulated bound.
- **Node-level bounding boxes are not worth it**: they avoid page touches the
  accumulated bound already removed.
- **Split on the widest axis**, not cycling by depth: slightly faster,
  identical recall, and it makes duplicate-heavy data buildable.
- **Cold is 600x warm** (4,575 ms vs 7.6 ms at k=1024) on `/work`. Prefault
  needs `MADV_SEQUENTIAL`: 53 s for 8.9 GB, 7x slower under `MADV_RANDOM`.

## Serving

Ranking on the fingerprint column before fetching SMILES drops database lookups
from 1,024 to 8; fetching all k to serve 8 cost 2.6 ms of 7.1 ms. Dropping the
descriptor clone from the heap and record materialization from the scan took
in-tree 5.05 to 3.93 ms. Together **k=1024 went 7.76 to 4.46 ms (43%)**, k=128
2.14 to 1.74 ms.

| k | served | wall | in-tree | rank |
|---|---|---|---|---|
| 128 | 8 | 1.74 ms | 1.50 ms | 36 us |
| 128 | 128 | 2.07 ms | 1.49 ms | 36 us |
| 1024 | 8 | **4.46 ms** | 3.93 ms | 263 us |
| 1024 | 1024 | 7.10 ms | 3.93 ms | 268 us |

- **Concurrency scales**: k=1024, 139 qps at one client to 2,975 at 32 (21.3x
  on 32 cores), median 6.8 to 9.4 ms, p99 13.8 to 30.5 ms, no errors. Nothing
  in the query path serializes. The k=128 plateau at ~5,500 qps is likely the
  GIL-bound Python client.
- **Everything the query path touches must be prefaulted, `db.db` included.**
  Warming only `record` and `fingerprints` left the first query at **2,396 ms**
  against 8 ms in-tree — one disk read per hit, 1,024 of them. Warmed, 6-21 ms.
- Prefaulting all 35.5 GB takes **12 s** on an uncontended `/work`. A 377 s
  reading was NFS contention.
- **Hits are not deduplicated, and need not be.** 33.7% of the corpus carries
  stereochemistry, and siblings are separate records with identical descriptors
  and identical atom-pair similarity, so they tie. Measured over 500 queries at
  k=1024 serving 8: 1.0% of queries return any sibling pair, 0.12% of served
  slots, never a group above 2 (`scripts/stereo_duplicates.py`). Stereoisomers
  are separately purchasable, so collapsing them would hide real compounds.

## On the production host

7950X, 128 GB, local NVMe, one GPU, 161M index.

| | |
|---|---|
| embed (GPU, one molecule) | 12.9 ms |
| project + fingerprint | 0.17 ms |
| index, warm | 3.0 ms |
| **end to end, warm** | **17.8 ms** |

- **Cold is 168x warm** (502 ms against 3.0 ms at k=1024), decaying to 72x by
  the fourth query. An earlier estimate of ~30x was wrong five-fold.
  `MADV_RANDOM` is right warm and wrong cold: it disables readahead.
- **Embedding is 72% of a query and a GPU barely helps**: 35 ms CPU against
  12.9 GPU. One molecule is a batch of one, so the cost is kernel launch, not
  compute. `torch.compile`, CUDA graphs or fp16 might move it.
- **Saturates at ~152 qps**, flat from 4 clients up, latency growing linearly
  (26 ms at 4, 104 ms at 16). Implies a **6.6 ms serialized section**: the
  model lock.
- **Batch-1 inference wastes the GPU**, so dynamic batching is the lever for
  concurrency — not more workers, not MPS. The GPU is underused, not contended.

| batch | 1 | 32 | 128 |
|---|---|---|---|
| ms | 8.3 | 14.7 | 40.9 |
| mol/s | 120 | 2,173 | 3,134 |

## Batch submission

Bulk throughput is not monotonic in batch size: every batch pads to its longest
SMILES. L40S, 40,000 REAL molecules (`scripts/batch_probe.py`):

| batch | 32 | 48 | **64** | 96 | 128 | 256 |
|---|---|---|---|---|---|---|
| mol/s | 2,278 | 2,665 | **2,791** | 2,761 | 2,483 | 2,683 |

Batch 64 sustained **3,871 mol/s** over the 1.6B run against 2,700 at batch
1024 for 161M: worth 43%. Sorting each shard by SMILES length would remove the
padding waste entirely — untried.

Batching the embedding is half the problem. At 10,000 molecules it drops
embedding 83 s to ~2.4 s, leaving the index the bottleneck at 30 s serialized,
so `/nn/batch/{k}` runs its queries across a thread pool. Preprocessing is not
a bottleneck: RDKit parses at 25,100 mol/s and fingerprints at 41,600, so
10,000 structures cost ~0.6 s on one core.

161M index, 14 index threads, embedding on CPU and the index cold on network
storage — embed is a floor, index a ceiling:

| submitted | prepare | embed (CPU) | index (cold) |
|---|---|---|---|
| 10 | 2 ms | 99 ms | 32 ms |
| 100 | 14 ms | 966 ms | 125 ms |
| 1000 | 126 ms | 10,020 ms | 740 ms |

**1,000 queries in 740 ms cold** against 30 s run one at a time. Warm on the
production host, 10,000 should cost ~2 s in the index and ~2.4 s on the GPU.

- **SALSA canonicalizes internally and discards stereochemistry.**
  `C[C@H](N)C(=O)O` and `CC(N)C(=O)O` embed to distance exactly 0.0, as do
  `OCC` and `CCO`. Deduplicating on the stereochemistry-free form costs
  nothing.
- `filter_input=True`, SALSA's default, silently drops rows it cannot embed and
  misaligns every row after the first. The service validates first, embeds with
  `filter_input=False`, and checks the returned count.

## Brute force, priced

Enamine In-Stock plus the supermutant set, 5,038,800 compounds, 1,000 anchor
queries, recall is the depth-d relative in the top 5. GPU is one L40S; CPU is
8 threads per task over a 100-task array. All five representations cover the
same rows, joined on compound ID.

| representation | width | GPU h/1M | CPU core-h/1M | GPU vs CPU | depth-1 | depth-2 |
|---|---|---|---|---|---|---|
| PCA-10 | 10 f32 | 0.25 | 91.1 | 364x | 0.500 | 0.142 |
| salsa16 | 16 f32 | 0.42 | 113.8 | 271x | 0.655 | 0.190 |
| salsa24 | 24 f32 | 0.50 | 131.0 | 262x | 0.654 | 0.168 |
| Morgan | 512 bit | 1.11 | 305.6 | 275x | 0.682 | 0.210 |
| **atom pairs** | 512 bit | 1.11 | 321.1 | 289x | **0.885** | **0.526** |
| **kd-tree k=128** | 10 f32 | — | **0.325** | — | **0.917** | 0.508 |
| kd-tree k=1024 | 10 f32 | — | 0.863 | — | 0.903 | 0.583 |

**The index beats exhaustive atom-pair search on both axes at once**: 0.325
core-hours per million queries against 321, and 91.7% depth-1 against 88.5%.
Scanning all 5,038,800 compounds against the full library costs 1,618 core-hours
or 5.6 GPU-hours by atom pairs, and 1.6 core-hours through the index — **988x**.
One CPU core running the index also beats a whole L40S scanning atom pairs.

- **Exhaustive embedding search is the worst of both.** PCA-10 brute force is
  the cheapest scan and the least accurate thing measured, at 50.0% depth-1
  against the index's 91.7% over the same vectors. Retrieval order is not
  ranking order; the index plus re-ranking is not an approximation of the scan,
  it is a better answer.
- **salsa24 costs more than salsa16 and matches it** (0.654 against 0.655), as
  at 161M.
- **Optimal k moves with library size.** At 5M, k=128 beats k=1024 on depth-1
  (91.7% against 90.3%) because a wider retrieval feeds the re-ranker more
  distractors; at 161M k=1024 wins decisively (98.0% against 82.7%). Depth-2
  still prefers the wider retrieval at both scales.
- GPU is 262-364x a CPU core, widest for the narrowest vectors.

## Metrics that did not work

MCS and Tanimoto averaged over the top 5 do not separate models at this scale
(1.19x and 1.28x against 2.04x for GED recall): they regress toward the
library's background similarity, high at 161M.

## Open model-side question

Re-ranking recovers ~98% of the depth-1 ceiling but only ~62% of depth-2 in
every model. SALSA's contrastive training uses only 1-edit pairs, so the loss
never constrains 2-edit ordering. Higher d keeps improving depth-2 (54.1% at
d=10, 60.3% at d=16), suggesting multi-GED training data pays.

## Measurement discipline

- **Cheap probes mispredicted the in-situ result five times**: depth-ordering
  correlation on salsa24, quantization distance error, top-10 overlap in a
  200k pool, AVX-512 width, retained variance across bases. Triage only.
- **Microbenchmarks overstated gains 1.75-2.5x, three times.**
- **Warm with the exact queries to be timed**, not a fixed prefix.
- **Fixing the dominant cost promotes the next one.** The scan was 31% and
  dismissed; after the frontier bound it was 65% and unchanged in absolute
  terms. Shares are not conclusions.
- **Check a new measurement against a known one.** 57 ms against a known 8.1 ms
  exposed a wrong query projection, not a slow index.
- **Run the tests in debug too.** Release wrapped a `len() - 1` underflow to
  `usize::MAX` and wrapped it back on read.
