import torch
from torch.utils.data import Dataset
from torch.utils.data import Sampler
from typing import Iterator, List, Optional, Sized

import numpy as np
import pandas as pd
from tqdm import tqdm


def undo_BrCl_singles(smi):
    smi = smi.replace('R', 'Br')
    return smi.replace('L', 'Cl')


def do_BrCl_singles(smi):
    smi = smi.replace('Br', 'R')
    return smi.replace('Cl', 'L')


def get_dataset_array(path, rdkvs_dataset=False, client_dataset=True):
    if rdkvs_dataset:
        ########
        df = pd.read_csv(path, header=None, sep='\t', compression='gzip')
        df.columns = ['0', '1', 'Smiles']
    if client_dataset:
        df = pd.read_csv(path)
    #         df.columns = ['0','1','Smiles']
    else:
        df = pd.read_csv(path)

    for col in ['Smiles', 'Is_anc', 'Anc_idx']:
        if col not in df.columns:
            if col == 'Is_anc':
                df[col] = True
            elif col == 'Anc_idx':
                df[col] = df.index
            elif col == 'Smiles':
                raise ValueError("No SMILES column in dataframe!")

    return df[['Smiles', 'Is_anc', 'Anc_idx']]


def get_anc_map(ds, n_augs):
    anc_map = {}
    for i in tqdm(range(0, len(ds), n_augs + 1), total=len(ds) // (n_augs + 1),
                  desc="Running 'get_anc_map'"):
        aug_set = ds.loc[i:i + n_augs]
        anc_idx = aug_set['Anc_idx'].values[0]
        anc_map[anc_idx] = aug_set.index.values
        assert (len(set(aug_set['Anc_idx'])) == 1)
        assert (aug_set.iloc[0]['Is_anc'] == True)
    return anc_map


def get_anc_map_fast(ds, n_augs):
    anc_idc = range(len(ds) // (n_augs + 1))
    anc_map = {i: np.arange((n_augs + 1) * i, (n_augs + 1) * (i + 1)) for i in anc_idc}
    return anc_map


def get_anc_map_explicit(ds):
    anc_idc = ds[ds.Is_anc == True].Anc_idx.to_numpy()
    anc_map = {i: np.where(ds.Anc_idx == i)[0] for i in anc_idc}
    return anc_map


class ContraSeqDataset(Dataset):
    def __init__(self, smiles, s_token='<', e_token='>', pad_token='X', max_len=150, prop_arr=None, vocab=None):
        super().__init__()

        ## Dataset array ##
        ds = pd.DataFrame({"Smiles": smiles})
        if max_len is not None:
            self.df = ds[ds['Smiles'].str.len() <= max_len]
        else:
            self.df = ds
            max_len = ds['Smiles'].str.len().max()
        if vocab is None: vocab = '#%()+-0123456789<=>BCFHILNOPRSX[]cnosp$'
        self.vocab = vocab
        tokens = list(set(vocab + s_token + e_token + pad_token))
        self.tokens = ''.join(list(np.sort(tokens)))
        self.s_token = s_token
        self.e_token = e_token
        self.p_token = pad_token
        self.n_tokens = len(self.tokens)
        self.max_sm_len = max_len
        self.max_len = max_len + 2

        ## Property array ##
        if prop_arr is None:
            self.prop_arr = torch.full((len(self.df), 1), -1)
        else:
            # Make sure indices match up
            np.testing.assert_array_equal(prop_arr[['Idx']].squeeze().values,
                                          prop_arr.index.to_series().values)
            # Drop place-holding index column
            prop_arr = prop_arr.drop('Idx', axis=1).values
            self.prop_arr = prop_arr
            self.prop_size = prop_arr.shape[1]

    def idc_tensor(self, smi):
        tensor = torch.zeros(len(smi)).long()
        for i in range(len(smi)):
            tensor[i] = self.tokens.index(smi[i])
        return tensor

    def get_vec(self, smi):
        padding = ''.join([self.p_token for _ in range(self.max_sm_len - len(smi))])
        smi = self.s_token + smi + self.e_token + padding
        vec = self.idc_tensor(smi)
        return vec

    def remove_extra_tokens(self, smi):
        smi = smi.replace(self.p_token, '')
        smi = smi.replace(self.s_token, '')
        return smi.replace(self.e_token, '')

    def replace_unks(self, smi):
        smi = ''.join([char if char in self.vocab else '$' for char in smi])
        return smi

    def undo_BrCl_singles(self, smi):
        smi = smi.replace('R', 'Br')
        return smi.replace('L', 'Cl')

    def do_BrCl_singles(self, smi):
        smi = smi.replace('Br', 'R')
        return smi.replace('Cl', 'L')

    def convert_vec_to_smi(self, vec, snip=False):
        smi_arr = np.array(list(self.tokens))[vec.cpu().detach().numpy()]
        smi_list = [''.join(arr) for arr in smi_arr]
        smi_list = [self.undo_BrCl_singles(smi) for smi in smi_list]
        if snip:
            smi_list = [self.remove_extra_tokens(smi) for smi in smi_list]
        return smi_list

    def masks(self, seq):
        p_idx = self.tokens.index(self.p_token)
        s_idx = self.tokens.index(self.s_token)
        e_idx = self.tokens.index(self.e_token)

        # pad mask: masks pad tokens
        pad_mask = (seq == p_idx)

        # avg mask: masks pad,s,e tokens
        avg_mask = ((seq == p_idx) | (seq == e_idx) | (seq == s_idx)).float()
        avg_mask = torch.ones_like(avg_mask) - avg_mask

        # sup (superfluous) mask: masks s,e tokens
        sup_mask = torch.ones(self.n_tokens)
        idx = torch.tensor([s_idx, p_idx])
        sup_mask = torch.zeros_like(sup_mask).scatter_(0, idx, sup_mask)
        sup_mask = sup_mask.unsqueeze(0)
        return pad_mask, avg_mask, sup_mask

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()

        smi = self.df.iloc[idx]["Smiles"]
        vec = self.get_vec(self.replace_unks(self.do_BrCl_singles(smi)))
        masks = self.masks(vec)
        props = self.prop_arr[idx]

        seq_attr = {'Seq': vec,
                    'Smiles': smi,
                    'Pad_mask': masks[0],
                    'Avg_mask': masks[1],
                    'Out_mask': masks[2],
                    'Props': props}
        #         print(seq_attr['Props'].shape)

        return seq_attr


class AnchoredSampler(Sampler[List[int]]):
    """
    Args:
        sampler (Sampler or Iterable): Base sampler. 
        batch_size (int): Size of mini-batch.
        drop_last (bool): If ``True``, the sampler will drop the last batch if
            its size would be less than ``batch_size``
    """
    def __init__(self, sampler: Sampler[int], anc_map: dict, batch_size: int, drop_last: bool,
                 data_source: Optional[Sized]) -> None:
        super().__init__(data_source)
        self.sampler = sampler
        self.anc_map = anc_map
        self.batch_size = batch_size
        self.drop_last = drop_last

    def __iter__(self) -> Iterator[List[int]]:
        batch = []
        i = 0
        for idx in self.sampler:
            augs = self.anc_map[idx].tolist()
            batch.extend(augs)
            i += 1
            if i % (self.batch_size) == 0:
                yield batch
                batch = []
        if len(batch) > 0 and not self.drop_last:
            yield batch
    
    def __len__(self) -> int:
        if self.drop_last:
            return len(self.sampler) // self.batch_size
        else:
            return (len(self.sampler) + self.batch_size - 1) // self.batch_size
