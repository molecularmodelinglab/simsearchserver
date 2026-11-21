import os

from salsa_lit import load_model
from salsa_lit.constants import MAX_SMI_LEN
from salsa_lit.data.tokenizer import SMILESTokenizer
from salsa_lit.pretrained import DEFAULT_MODEL, list_pretrained
from salsa_lit.utils.chemistry import get_cansmiles

import torch

MODEL_ENV = "SALSA_MODEL"


class SalsaModel:

    def __init__(self, model=None, device="cpu"):

        self.model_name = model or os.environ.get(MODEL_ENV) or DEFAULT_MODEL
        self.device = torch.device(device)

        self.model = load_model(self.model_name, map_location=str(self.device))
        self.model.to(self.device)

        self.tokenizer = SMILESTokenizer()

        registry = list_pretrained()

        if self.model_name in registry:
            self.dim = registry[self.model_name]["dim_salsa"]
        else:
            self.dim = len(self.embed("CCO"))

    def embed(self, smiles):

        canonical = get_cansmiles(smiles)

        if canonical == "":
            raise ValueError(f"Could not parse SMILES: {smiles!r}")

        if len(canonical) > MAX_SMI_LEN:
            raise ValueError(
                f"SMILES is {len(canonical)} characters, over the {MAX_SMI_LEN} "
                f"the model was trained on"
            )

        batch = self.tokenizer.batch_encode([canonical])

        with torch.no_grad():
            latent, _ = self.model.embed(
                batch["seq"].to(self.device),
                batch["pad_mask"].to(self.device),
                batch["avg_mask"].to(self.device),
                batch["out_mask"].to(self.device),
            )

        return [float(x) for x in latent[0].cpu().numpy()]
