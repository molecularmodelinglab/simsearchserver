import torch
import numpy as np

VOCAB = '#%()+-0123456789<=>BCFHILNOPRSX[]cnosp$'
N_TOKENS = 39 # ... is len(VOCAB)
S_TOKEN = '<' # start token
E_TOKEN = '>' # end token
P_TOKEN = 'X' # pad token
_tokens = list(set(VOCAB + S_TOKEN + E_TOKEN + P_TOKEN))
TOKENS = ''.join(list(np.sort(_tokens)))
MAX_SMI_LEN = 120
MAX_VEC_LEN = 122
NAMED_LOSSES = ['Recon','SupCon']

def process_smiles(smiles):

    vec = _get_vec(_replace_unks(_do_BrCl_singles(smiles)) )
    pad_mask, avg_mask, _ = _get_masks_from_vec(vec)

    inputs = {}
    inputs["seq"] = vec
    inputs["pad_mask"] = pad_mask
    inputs["avg_mask"] = avg_mask

    type_map = {
        "seq": np.float32,
        "pad_mask": np.float32,
        "avg_mask": np.float32,
        "out_mask": np.float32,
        }

    typed = {}
    for key, value in inputs.items():
        try:
            typed[key] = value.cpu().numpy().astype(type_map[key])
        except:
            typed[key] = value

    big_mat = np.stack([typed["seq"], typed["pad_mask"], typed["avg_mask"]])

    #trim to single batch size
    #big_mat = big_mat[:,0,:]
    big_mat = big_mat.reshape(big_mat.shape[0], 1, big_mat.shape[1])

    return big_mat


def _remove_extra_tokens(smi):
    smi = smi.replace(P_TOKEN,'')
    smi = smi.replace(S_TOKEN,'')
    smi = smi.replace(E_TOKEN,'')
    return smi

def _do_BrCl_singles(smi):
    smi = smi.replace('Br','R')
    smi = smi.replace('Cl','L')   
    return smi

def _replace_unks(smi):
    smi = ''.join([char if char in VOCAB else '$' for char in smi])
    return smi

def _get_vec(smi):
    padding = ''.join([P_TOKEN for _ in range(MAX_SMI_LEN - len(smi))])
    smi = S_TOKEN + smi + E_TOKEN + padding    
    vec = torch.zeros(len(smi)).long()
    for i in range(len(smi)): 
        vec[i] = TOKENS.index(smi[i])    
    return vec

def _get_masks_from_vec(vec):
    p_idx = TOKENS.index(P_TOKEN)
    s_idx = TOKENS.index(S_TOKEN)
    e_idx = TOKENS.index(E_TOKEN)
    # pad mask: masks pad tokens
    pad_mask = (vec==p_idx)
    # avg mask: masks pad,s,e tokens
    avg_mask = ((vec==p_idx)|(vec==e_idx)|(vec==s_idx)).float()
    avg_mask = torch.ones_like(avg_mask) - avg_mask
    # sup (superfluous) mask: masks s,e tokens
    sup_mask = torch.ones(len(TOKENS))
    idx = torch.tensor([s_idx, p_idx])
    sup_mask = torch.zeros_like(sup_mask).scatter_(0,idx,sup_mask)
    sup_mask = sup_mask.unsqueeze(0)
    return pad_mask, avg_mask, sup_mask  


