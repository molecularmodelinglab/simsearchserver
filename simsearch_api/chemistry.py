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
    """A SMILES RDKit could not parse."""


class Chemistry:
    """The SALSA model and the fingerprint generator, loaded once."""

    def __init__(self, settings):
        from salsa_lit.api.inference import embed_smiles, load_model

        self._embed_smiles = embed_smiles
        self.model_name = settings["model"]
        self.revision = settings["revision"]
        self.device = settings["device"]

        self._model = load_model(self.model_name, revision=self.revision,
                                 map_location=self.device)
        # Inference is not reentrant, and one model is all a query needs.
        self._lock = threading.Lock()

        self._generator = rdFingerprintGenerator.GetAtomPairGenerator(
            fpSize=FINGERPRINT_BITS, countSimulation=False)

        self.dim = len(self.embed("CCO"))

    def parse(self, smiles):
        """A molecule, or `InvalidSmiles`."""

        molecule = Chem.MolFromSmiles(smiles)

        if molecule is None:
            raise InvalidSmiles(f"could not parse {smiles!r} as a molecule")

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
