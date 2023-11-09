import onnxruntime as ort
import pickle
import time
import numpy as np
import torch

from modeling.preprocess import process_smiles
from rdkit.Chem.AllChem import GetHashedMorganFingerprint

from rdkit import Chem

class SalsaONNXModel():

    def __init__(self, path):

        #read in ONNX model and set up environment
        self.sess = ort.InferenceSession(path, providers = ["CPUExecutionProvider"])

    def embed(self, smiles):

        input = process_smiles(smiles)

        outputs = self.sess.run(None, {"big_mat": input})

        embedding = outputs[0]
        embedding = embedding.reshape((embedding.shape[1]))
        embedding = [float(x) for x in embedding]

        return embedding

class MorganPCAModel():

    def __init__(self):


        import pickle

        pca_filename = "/data/Enamine_REAL_DATASET_PCA/pca_model_1024.pkl"
        pca_model = pickle.load(open(pca_filename, 'rb'))
        self.pca_model = pca_model

    def _get_fp(self, smiles):

        mol = Chem.MolFromSmiles(smiles)
        fp = np.array(list(GetHashedMorganFingerprint(mol, radius=3, nBits=1024)))

        fp = fp.reshape((1, fp.shape[0]))
        print(fp)

        return fp


    def embed(self, smiles):
        print("embed")


        fp = self._get_fp(smiles)

    
        small = self.pca_model.transform(fp)

        return list(small[0])




