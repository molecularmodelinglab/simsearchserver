from copy import deepcopy
from typing import Optional, Union

import pickle
import yaml

import torch
from torch.utils.data import DataLoader

from python_web_server.smallsa.contra_seq_dataset import ContraSeqDataset
from python_web_server.smallsa.modules import SeqAutoencoder as SeqAE

import numpy as np
import pandas as pd
import time
import os

DEFAULT_SALSA_ARGS = {
    "max_len": 152,
    "dim_emb": 512,
    "heads": 8,
    "dim_hidden": 32,
    "L_enc": 6,
    "L_dec": 6,
    "dim_ff": 2048,
    "drpt": 0.1,
    "actv": 'relu',
    "eps": 0.6,
    "b_first": True,
    "num_props": None,
    "vocab": '#%()+-0123456789<=>BCFHILNOPRSX[]cnosp$'
}

SALSA_MODEL_LITERAL = ["n_tokens", "max_len", "dim_emb", "heads", "dim_hidden", "L_enc", "L_dec", "dim_ff",
                       "drpt", "actv", "eps", "b_first", "num_props"]

MAX_LEN_CONTRA_SEQ = 150


class SalsaArgs:
    def __init__(self, args: Optional[dict] = None):
        if args is None:
            self._args = DEFAULT_SALSA_ARGS
        else:
            self._args = args
        self._process_arg_to_attributes()

    def from_yaml(self, yaml_file, model_id):
        with open(yaml_file, "r") as f:
            _tmp_args = yaml.safe_load(f)[model_id]
        self._args = _tmp_args
        self._process_arg_to_attributes()

    def set_argument(self, arg_name, arg_val):
        self._args[arg_name] = arg_val
        self._process_arg_to_attributes()

    def delete_argument(self, arg_name):
        del self._args[arg_name]
        self._process_arg_to_attributes()

    def get_arguments(self):
        return self._args

    def _set_n_tokens(self):
        if "vocab" in self._args:
            self._args["n_tokens"] = len(self._args["vocab"])

    def _process_arg_to_attributes(self):
        self._set_n_tokens()
        for key, val in self._args.items():
            self.__setattr__(key, val)
        # remove no longer present arguments
        _copy = deepcopy(self.__dict__)
        for key in _copy.keys():
            if not key.startswith("_") and key not in self._args.keys():
                self.__delattr__(key)

    def get_model_arguments(self):
        _tmp = {}
        for key, val in self._args.items():
            if key in SALSA_MODEL_LITERAL:
                _tmp[key] = val
        return _tmp


def get_latents(query,
                model_args: Union[SalsaArgs, None, dict] = None,
                model_loc: Optional[str] = None,
                out_path: Optional[str] = None,
                batch_size: int = 1,
                use_cuda: Optional[bool] = True):
    if not use_cuda:
        torch.set_num_threads(1)
    if model_args is None:
        model_args = SalsaArgs()
    elif isinstance(model_args, dict):
        model_args = SalsaArgs(model_args)

    if not hasattr(model_args, "model_loc"):
        if model_loc is not None:
            model_args.set_argument("model_loc", model_loc)
        else:
            raise ValueError("cannot find location of model. pass as 'model_loc' or add to model args")

    model = SeqAE(**model_args.get_model_arguments())

    device = "cuda" if use_cuda else "cpu"

    if device == "cuda" and not torch.cuda.is_available():
        device = "cpu"

    model.load_state_dict(torch.load(model_loc), strict=False)
    model.to(device)
    model = model.eval()

    # ContraSeqDataset takes in a list of smiles

    ds = ContraSeqDataset(query, prop_arr=None, max_len=150, vocab='#%()+-0123456789<=>BCFHILNOPRSX[]cnosp$')
    loader = DataLoader(ds, batch_size=1, sampler=list(range(len(ds))), num_workers=0, pin_memory=True)
    latents = []
    val_idc = []
    for i, samp in enumerate(loader):
        t0 = time.time()
        for k, v in samp.items():
            if torch.is_tensor(v): samp[k] = v.to(device)
        inputs = [samp[x] for x in ['Seq', 'Pad_mask', 'Avg_mask', 'Out_mask']]
        latent = model.latent(*inputs)
        latent = latent.cpu().detach().numpy()
        latents.append(latent)
        val_idc.append([i])

    return np.concatenate(latents, axis=0)


def do_query(query, k=100):
    query = [query]
    model_args = SalsaArgs()
    model_args.from_yaml(f"{os.path.dirname(__file__)}/../models/models.yaml", "00_184474.pt")
    model_loc = f"{os.path.dirname(__file__)}/../models/00_184474.pt"
    latent = get_latents(query, model_args=model_args, model_loc=model_loc)

    tree = pickle.load(open(f"{os.path.dirname(__file__)}/chembl_tree_6.pkl", "rb"))
    dataset_smiles = pickle.load(open(f"{os.path.dirname(__file__)}/chembl_smiles.pkl", "rb"))

    dists, idx = tree.query(latent, k=k)

    print(idx)
    smiles = dataset_smiles[idx]

    dist_from_origin = np.linalg.norm(np.array([0, 0, 0, 0, 0, 0]) - latent)

    return pd.DataFrame({"SMILES": smiles.squeeze(), "Distance": np.round(dists.squeeze(), 6)}), dist_from_origin


if __name__ == "__main__":
    print(do_query("CCCCCCCC"))
