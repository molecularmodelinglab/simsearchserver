"""The three preparation stages, each resumable and shard-addressable."""

import glob
import hashlib
import json
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from simsearch import config as cfg

PCA_FILENAME = "pca.npz"


def shards(config):
    """Corpus shards in a fixed order, which is the order the index is built in."""

    path = config["corpus"]["input"]

    if os.path.isdir(path):
        found = sorted(glob.glob(os.path.join(path, "*.parquet")))
        if not found:
            found = sorted(glob.glob(os.path.join(path, "*.csv")))
        if not found:
            raise FileNotFoundError(f"no parquet or csv shards under {path}")
        return found

    if not os.path.exists(path):
        raise FileNotFoundError(path)

    return [path]


def assigned(items, shard, of):
    """The slice of `items` this task owns, for a slurm array."""
    return items[shard::of]


def _output_for(directory, source):
    base = os.path.splitext(os.path.basename(source))[0]
    return os.path.join(directory, f"{base}.parquet")


def _id_digest(ids):
    return hashlib.sha256("\n".join(ids).encode()).hexdigest()


def embed(config, shard=0, of=1):
    """Embeds each corpus shard with SALSA, skipping ones already done."""

    from salsa_lit.api.inference import load_model, serve_salsa

    directory = cfg.stage_directory(config, "embedding")
    os.makedirs(directory, exist_ok=True)

    todo = [s for s in assigned(shards(config), shard, of)
            if not os.path.exists(_output_for(directory, s))]

    if not todo:
        print("embedding: nothing to do")
        return

    settings = config["embedding"]
    # Loaded once; serve_salsa would otherwise reload it per shard.
    model = load_model(settings["model"], revision=settings["revision"])

    for source in todo:
        target = _output_for(directory, source)
        serve_salsa(source, model=model, output_file=target,
                    batch_size=settings["batch_size"], device=settings["device"])
        print(f"embedding: {source} -> {target}")


def _embedding_columns(path):
    names = pq.ParquetFile(path).schema_arrow.names
    return [n for n in names if n.startswith("dim_")]


def _read_embeddings(path, columns):
    table = pq.read_table(path, columns=columns)
    return np.stack([table[c].to_numpy() for c in columns], axis=1)


def fit_projection(config):
    """Fits the PCA basis on a sample spread across every shard."""

    directory = cfg.stage_directory(config, "projection")
    os.makedirs(directory, exist_ok=True)
    target = os.path.join(directory, PCA_FILENAME)

    if os.path.exists(target):
        print(f"projection: {target} already fitted")
        return target

    sources = sorted(glob.glob(os.path.join(
        cfg.stage_directory(config, "embedding"), "*.parquet")))
    if not sources:
        raise FileNotFoundError("no embedded shards to fit on; run embed first")

    columns = _embedding_columns(sources[0])
    settings = config["projection"]

    # A prefix of the corpus can be one narrow band of molecules, so every
    # shard contributes.
    per_shard = max(1, settings["fit_rows"] // len(sources))
    rng = np.random.default_rng(settings["seed"])

    blocks = []
    for source in sources:
        rows = _read_embeddings(source, columns)
        take = min(per_shard, len(rows))
        blocks.append(rows[rng.choice(len(rows), take, replace=False)])

    sample = np.concatenate(blocks)
    mean = sample.mean(axis=0)
    values, vectors = np.linalg.eigh(np.cov(sample - mean, rowvar=False))
    order = np.argsort(values)[::-1]

    components = vectors[:, order]
    variances = values[order]
    retained = float(np.cumsum(variances)[settings["dim"] - 1] / variances.sum())

    np.savez(target, mean=mean, components=components, variances=variances)
    print(f"projection: fitted on {len(sample):,} embeddings, "
          f"d={settings['dim']} retains {retained:.3%}")

    return target


def project(config, shard=0, of=1):
    """Projects embedded shards onto the fitted basis."""

    if config["projection"]["method"] == "none":
        print("projection: method is none, nothing to do")
        return

    model_path = fit_projection(config)
    loaded = np.load(model_path)
    mean, components = loaded["mean"], loaded["components"]
    dim = config["projection"]["dim"]

    directory = cfg.stage_directory(config, "projection")
    sources = sorted(glob.glob(os.path.join(
        cfg.stage_directory(config, "embedding"), "*.parquet")))

    corpus = config["corpus"]
    columns = _embedding_columns(sources[0])

    for source in assigned(sources, shard, of):
        target = _output_for(directory, source)
        if os.path.exists(target):
            continue

        table = pq.read_table(source)
        projected = (_read_embeddings(source, columns) - mean) @ components[:, :dim]

        arrays = [table[corpus["id_column"]], table[corpus["smiles_column"]]]
        names = [corpus["id_column"], corpus["smiles_column"]]
        for i in range(dim):
            arrays.append(pa.array(projected[:, i].astype(np.float32)))
            names.append(f"dim_{i}")

        pq.write_table(pa.table(arrays, names=names), target)
        print(f"projection: {source} -> {target}")


def index_source_directory(config):
    """The stage whose output the index is built from."""

    if config["projection"]["method"] == "none":
        return cfg.stage_directory(config, "embedding")

    return cfg.stage_directory(config, "projection")


def fingerprint(config, shard=0, of=1, processes=0):
    """Fingerprints the shards the index is built from, in their row order."""

    import multiprocessing as mp

    directory = cfg.stage_directory(config, "fingerprint")
    os.makedirs(directory, exist_ok=True)

    # Row order is the contract, so this reads what the builder reads rather
    # than the corpus: embedding drops over-length SMILES.
    sources = sorted(glob.glob(os.path.join(index_source_directory(config), "*.parquet")))
    if not sources:
        raise FileNotFoundError("no shards to fingerprint; run embed and project first")

    settings = config["fingerprint"]
    processes = processes or len(os.sched_getaffinity(0))
    corpus = config["corpus"]

    for source in assigned(sources, shard, of):
        base = os.path.splitext(os.path.basename(source))[0]
        target = os.path.join(directory, f"{base}.fp")
        manifest_path = os.path.join(directory, f"{base}.json")

        if os.path.exists(manifest_path):
            continue

        table = pq.read_table(source, columns=[corpus["id_column"], corpus["smiles_column"]])
        smiles = table.column(corpus["smiles_column"]).to_pylist()
        digest = _id_digest(table.column(corpus["id_column"]).to_pylist())
        del table

        chunks = [smiles[i:i + 20_000] for i in range(0, len(smiles), 20_000)]
        with mp.Pool(processes, initializer=_init_generator,
                     initargs=(settings,)) as pool:
            results = pool.map(_fingerprint_chunk, chunks, chunksize=1)

        packed = np.concatenate([block for block, _ in results])
        failed = sum(count for _, count in results)

        partial = target + ".partial"
        with open(partial, "wb") as handle:
            handle.write(packed.tobytes())
        os.replace(partial, target)

        with open(manifest_path, "w") as handle:
            json.dump({
                "rows": len(packed),
                "bytes_per_record": settings["bits"] // 8,
                "failed_parses": failed,
                "id_digest": digest,
                "source": source,
                **settings,
            }, handle, indent=2)

        print(f"fingerprint: {target}, {len(packed):,} records, {failed} failed")


_GENERATOR = None
_WIDTH = None


def _init_generator(settings):
    global _GENERATOR, _WIDTH
    from rdkit import RDLogger
    from rdkit.Chem import rdFingerprintGenerator

    RDLogger.DisableLog("rdApp.*")

    if settings["type"] == "atompair":
        _GENERATOR = rdFingerprintGenerator.GetAtomPairGenerator(
            fpSize=settings["bits"], countSimulation=settings["count_simulation"])
    else:
        _GENERATOR = rdFingerprintGenerator.GetMorganGenerator(
            radius=2, fpSize=settings["bits"], countSimulation=settings["count_simulation"])

    _WIDTH = settings["bits"] // 8


def _fingerprint_chunk(smiles):
    from rdkit import Chem, DataStructs

    out = np.zeros((len(smiles), _WIDTH), dtype=np.uint8)
    failed = 0

    for i, text in enumerate(smiles):
        molecule = Chem.MolFromSmiles(text)
        if molecule is None:
            failed += 1
            continue
        packed = DataStructs.BitVectToBinaryText(_GENERATOR.GetFingerprint(molecule))
        out[i] = np.frombuffer(packed, dtype=np.uint8)

    return out, failed
