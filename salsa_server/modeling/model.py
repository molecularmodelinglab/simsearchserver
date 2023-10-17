import onnxruntime as ort
import pickle
import time
import numpy as np
import torch

from modeling.preprocess import process_smiles

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

