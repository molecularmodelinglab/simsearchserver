"""Turns a SMILES into what the index needs: a descriptor and a fingerprint.

The fingerprint must be bit-identical to the column the index stores, so it is
generated here with RDKit rather than in the browser.
"""

import threading

import numpy as np
from rdkit import Chem, DataStructs, RDLogger
from rdkit.Chem import rdFingerprintGenerator

RDLogger.DisableLog("rdApp.*")

FINGERPRINT_BITS = 512


class InvalidSmiles(Exception):
    """A query the model cannot embed, with a reason worth showing."""


class ModelUnavailable(Exception):
    """The model could not be loaded, with the remedy named."""


class Chemistry:
    """The SALSA model and the fingerprint generator, loaded once."""

    def __init__(self, settings):
        from salsa_lit.api.inference import embed_smiles, load_model
        from salsa_lit.constants import MAX_SMI_LEN

        self._embed_smiles = embed_smiles
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

    def parse(self, smiles):
        """A molecule the model can embed, or `InvalidSmiles` saying why not."""

        if len(smiles) > self.max_smiles_length:
            raise InvalidSmiles(
                f"SMILES is {len(smiles)} characters; this model embeds up to "
                f"{self.max_smiles_length}")

        molecule = Chem.MolFromSmiles(smiles)

        if molecule is None:
            raise InvalidSmiles(f"could not parse {smiles!r} as a molecule")

        if len(Chem.GetMolFrags(molecule)) > 1:
            raise InvalidSmiles(
                "this is a multi-fragment structure, such as a salt or mixture; "
                "search the parent structure on its own")

        return molecule

    def embed(self, smiles):
        """The SALSA embedding of one SMILES."""

        with self._lock:
            vectors = self._embed_smiles([smiles], self._model, device=self.device)

        return np.asarray(vectors, dtype=np.float32).reshape(-1)

    def fingerprint(self, molecule):
        """The query fingerprint, packed as the index stores them."""

        packed = DataStructs.BitVectToBinaryText(
            self._generator.GetFingerprint(molecule))

        return np.frombuffer(packed, dtype=np.uint8)

    def canonical(self, molecule):
        return Chem.MolToSmiles(molecule)
