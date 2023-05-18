
from rdkit.Chem import AllChem, DataStructs
import pickle
import numpy as np

def get_morgan_descriptor(mol, radius = 2, convert_to_np = True):

    fp = AllChem.GetMorganFingerprintAsBitVect(mol, radius)

    if convert_to_np:
        arr = np.array((0,))
        DataStructs.ConvertToNumpyArray(AllChem.GetMorganFingerprintAsBitVect(mol, radius), arr)
        arr = np.array(arr, dtype=np.float32)
        arr = arr.reshape(1, -1)
        return arr

    
    return fp

def embed_morgan_pca_16(mol):
    transformer = pickle.load(open('../embeddings/morgan_pca_16/pca_16_transformer.pkl', 'rb'))
    embedding = transformer.transform(get_morgan_descriptor(mol))[0,:]

    return embedding

def embed_smallsa_16():
    pass

def embed_smallsa_8():
    pass


