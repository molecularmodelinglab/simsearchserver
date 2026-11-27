"""Turns a SMILES into what the index needs: a descriptor and a fingerprint.

The fingerprint must be bit-identical to the column the index stores, so it is
generated here with RDKit rather than in the browser.
"""

import threading
from dataclasses import dataclass

import numpy as np
from rdkit import Chem, DataStructs, RDLogger
from rdkit.Chem import rdFingerprintGenerator

RDLogger.DisableLog("rdApp.*")

FINGERPRINT_BITS = 512

# Measured peak; larger batches lose more to padding than they gain.
EMBED_BATCH = 64

# How often `embed_many` releases the model lock.
EMBED_CHUNK = 512


class InvalidSmiles(Exception):
    """A query the model cannot embed, with a reason worth showing."""


class ModelUnavailable(Exception):
    """The model could not be loaded, with the remedy named."""


@dataclass(frozen=True)
class Prepared:
    """One validated query, and the stereochemistry-free form it dedups on."""

    smiles: str
    canonical: str
    fingerprint: np.ndarray
    note: str | None = None


class Chemistry:
    """The SALSA model and the fingerprint generator, loaded once."""

    def __init__(self, settings):
        from salsa_lit.api.inference import load_model, serve_salsa
        from salsa_lit.constants import MAX_SMI_LEN

        self._serve = serve_salsa
        self.max_smiles_length = MAX_SMI_LEN
        self.model_name = settings["model"]
        self.revision = settings["revision"]
        self.device = settings["device"]
        self.cache_dir = settings["cache_dir"]

        try:
            self._model = load_model(self.model_name, revision=self.revision,
                                     map_location=self.device,
                                     cache_dir=self.cache_dir)
        except PermissionError as error:
            raise ModelUnavailable(
                f"cannot write the model cache at {error.filename}. Either fix "
                f"its ownership (it is often left root-owned by an earlier sudo "
                f"run) or point embedding.cache_dir at a writable directory."
            ) from error
        except OSError as error:
            raise ModelUnavailable(
                f"could not load {self.model_name}: {error}. Set "
                f"embedding.cache_dir to a writable directory, or pre-download "
                f"the model with `salsa-lit download`."
            ) from error
        # Inference is not reentrant, and one model is all a query needs.
        self._lock = threading.Lock()

        self._generator = rdFingerprintGenerator.GetAtomPairGenerator(
            fpSize=FINGERPRINT_BITS, countSimulation=False)

        self.dim = len(self.embed("CCO"))

    def prepare(self, smiles):
        """A validated query, or `InvalidSmiles` saying why not."""

        if len(smiles) > self.max_smiles_length:
            raise InvalidSmiles(
                f"SMILES is {len(smiles)} characters; this model embeds up to "
                f"{self.max_smiles_length}")

        molecule = Chem.MolFromSmiles(smiles)

        if molecule is None:
            raise InvalidSmiles(f"could not parse {smiles!r} as a molecule")

        note = None
        fragments = Chem.GetMolFrags(molecule, asMols=True)

        if len(fragments) > 1:
            molecule = max(fragments, key=lambda fragment: fragment.GetNumHeavyAtoms())
            note = "reduced to its largest fragment"

        canonical = Chem.MolToSmiles(molecule, isomericSmiles=False)

        if len(canonical) > self.max_smiles_length:
            raise InvalidSmiles(
                f"the canonical form is {len(canonical)} characters; this model "
                f"embeds up to {self.max_smiles_length}")

        return Prepared(smiles=smiles, canonical=canonical,
                        fingerprint=self.fingerprint(molecule), note=note)

    def embed(self, smiles):
        """The SALSA embedding of one SMILES."""

        return self.embed_many([smiles])[0]

    def embed_many(self, smiles, chunk=EMBED_CHUNK):
        """Embeddings for a list of SMILES, one row each, in the order given."""

        if not smiles:
            return np.zeros((0, self.dim), dtype=np.float32)

        blocks = []

        for start in range(0, len(smiles), chunk):
            window = smiles[start:start + chunk]

            # filter_input would drop rows and misalign every row after them.
            with self._lock:
                vectors, returned = self._serve(
                    window, self._model, batch_size=EMBED_BATCH,
                    filter_input=False, device=self.device)

            if len(returned) != len(window):
                raise RuntimeError(
                    f"model returned {len(returned)} embeddings for {len(window)} "
                    f"molecules, so rows no longer correspond")

            blocks.append(np.asarray(vectors, dtype=np.float32).reshape(len(window), -1))

        return np.concatenate(blocks)

    def fingerprint(self, molecule):
        """The query fingerprint, packed as the index stores them."""

        packed = DataStructs.BitVectToBinaryText(
            self._generator.GetFingerprint(molecule))

        return np.frombuffer(packed, dtype=np.uint8)
