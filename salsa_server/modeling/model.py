import os

from salsa_lit import load_model
from salsa_lit.data.tokenizer import SMILESTokenizer
from salsa_lit.utils.chemistry import get_cansmiles

import torch

MODEL_PATH_ENV = "SALSA_MODEL_PATH"


class SalsaModel:

    def __init__(self, path=None, device="cpu"):

        path = path or os.environ.get(MODEL_PATH_ENV)

        if not path:
            raise RuntimeError(
                f"No model path given. Set {MODEL_PATH_ENV} to a salsa checkpoint "
                f"or model directory."
            )

        self.path = path
        self.device = torch.device(device)

        self.model = load_model(path, map_location=str(self.device))
        self.model.to(self.device)

        self.tokenizer = SMILESTokenizer()

        # Determine the embedding dimension by embedding a simple molecule
        self.dim = len(self.embed("CCO"))

    def embed(self, smiles):

        canonical = get_cansmiles(smiles)

        if canonical == "":
            raise ValueError(f"Could not parse SMILES: {smiles!r}")

        batch = self.tokenizer.batch_encode([canonical])

        with torch.no_grad():
            latent, _ = self.model.embed(
                batch["seq"].to(self.device),
                batch["pad_mask"].to(self.device),
                batch["avg_mask"].to(self.device),
                batch["out_mask"].to(self.device),
            )

        return [float(x) for x in latent[0].cpu().numpy()]
