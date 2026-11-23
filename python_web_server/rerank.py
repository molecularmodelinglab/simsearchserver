"""Structural re-ranking of the candidates the kd-tree returned.

The embedding is good at retrieval and poor at ranking: measured on 161M
Enamine REAL compounds, the depth-1 GED relative of a query is in the top 5 by
embedding distance 21% of the time, but is somewhere in the retrieved 1024 72%
of the time. Re-ranking those candidates by structural similarity recovers
almost all of that, 71%.

Atom pairs at 512 bits with Tanimoto came out of a sweep of 13 fingerprints and
5 similarity metrics.
"""

import os
import tempfile

import numpy as np
from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator, rdmolfiles

FP_BITS = int(os.environ.get("RERANK_FP_BITS", "512"))

PARSE_THREADS = int(os.environ.get("RERANK_PARSE_THREADS", "0")) or len(os.sched_getaffinity(0))

# Below this, the thread pool and its temporary file cost more than they save.
PARSE_MIN_BATCH = int(os.environ.get("RERANK_PARSE_MIN_BATCH", "64"))

_TMPDIR = "/dev/shm" if os.path.isdir("/dev/shm") else tempfile.gettempdir()

_FPGEN = rdFingerprintGenerator.GetAtomPairGenerator(fpSize=FP_BITS)


def parse_smiles(smiles_list):
    """Molecules for `smiles_list`, in order, `None` where parsing failed."""

    if len(smiles_list) < PARSE_MIN_BATCH or PARSE_THREADS < 2:
        return [Chem.MolFromSmiles(s) for s in smiles_list]

    path = None
    try:
        fd, path = tempfile.mkstemp(suffix=".smi", dir=_TMPDIR)
        with os.fdopen(fd, "w") as handle:
            handle.write("\n".join(f"{s} {i}" for i, s in enumerate(smiles_list)))

        mols = [None] * len(smiles_list)
        supplier = rdmolfiles.MultithreadedSmilesMolSupplier(
            path, delimiter=" ", smilesColumn=0, nameColumn=1,
            titleLine=False, numWriterThreads=PARSE_THREADS)

        for mol in supplier:
            if mol is not None:
                mols[int(mol.GetProp("_Name"))] = mol

        return mols

    except Exception:
        return [Chem.MolFromSmiles(s) for s in smiles_list]

    finally:
        if path is not None:
            try:
                os.unlink(path)
            except OSError:
                pass


def fingerprint(mol):
    return _FPGEN.GetFingerprint(mol)


def rerank(query_mol, hits, num_neighbors):
    """The `num_neighbors` hits most similar to `query_mol`, best first.

    Each returned hit gains a `similarity` field. Hits whose SMILES RDKit
    cannot parse are dropped rather than ranked arbitrarily; if none parse,
    the kd-tree's own ordering is returned untouched so a query still answers.
    """

    if not hits:
        return []

    parsed = parse_smiles([hit["smiles"] for hit in hits])

    mols = [m for m in parsed if m is not None]
    kept = [h for h, m in zip(hits, parsed) if m is not None]

    if not mols:
        return hits[:num_neighbors]

    fps = _FPGEN.GetFingerprints(mols, numThreads=-1)
    sims = np.asarray(
        DataStructs.BulkTanimotoSimilarity(fingerprint(query_mol), list(fps)),
        dtype=np.float32,
    )

    k = min(num_neighbors, len(sims))
    # argpartition finds the k best in linear time; only those k are sorted.
    top = np.argpartition(-sims, k - 1)[:k]
    top = top[np.argsort(-sims[top], kind="stable")]

    ranked = []
    for i in top:
        hit = dict(kept[i])
        hit["similarity"] = float(sims[i])
        ranked.append(hit)

    return ranked
