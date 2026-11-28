# Measurements

The raw output behind NOTES.md. The indexes these were measured on have been
deleted; these files are the only surviving record.

| File | What it measured |
|---|---|
| `pca24_dim_sweep.json` | d8–d24 GED recall on the refit basis — chose d10 |
| `pca_prefit_dim_sweep.json` | the same sweep on the pre-refit basis |
| `rerank.json` | retrieve depth vs served count — chose k=1024 |
| `fingerprint_sweep.json` | fingerprint type, bit width and Tversky alpha/beta |
| `quantization.json` | int3–int8 descriptors against fp32 — rejected |
| `page_sweep_salsa8.json`, `page_sweep_salsa16.json` | record page size at 4M records |
| `page_sweep_salsa8_early.json` | the first page sweep, cold-read only |
| `remeasured_salsa16.json` | page size re-measured after the frontier bound fix |
| `accuracy_r161m.json` | GED recall at 161M |
| `scaling.json`, `scaling_warm.json` | latency vs record count, cold and warm |
| `baseline_before_refactor.json`, `benchmark.json` | latency before and after the bound and heap work |
| `pca_strat_basis.npz` | the shipping PCA basis |

`pca_strat_basis.npz` holds `mean` (16), `components` (16, 16) and `variances`
(16) for salsa16, with one component per column. Project to d with
`(x - mean) @ components[:, :d]` — verified to reproduce the `components`
served at `/transform` by `data/index/prod/d10`, whose digest is recorded in
that index's `config.resolved.yaml`.
