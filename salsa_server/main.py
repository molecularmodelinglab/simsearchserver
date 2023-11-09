import sys
sys.path.append(".")
from flask import Flask, render_template, request, abort, Response, jsonify
import threading

from json import dumps

from modeling.model import SalsaONNXModel, MorganPCAModel

app = Flask(__name__)

#model = SalsaONNXModel("modeling/models/salsa_bigmat.onnx")
model = MorganPCAModel()
model_lock = threading.Lock()

@app.route('/')
def home():
    print("main")

@app.route('/smiles/<smiles_value>', methods=['GET', 'POST'])
def smiles(smiles_value):

    print("SMILES: ", smiles_value)

    print(type(smiles_value))

    data = None
    try:
        model_lock.acquire()
        data = model.embed(smiles_value)
        model_lock.release()
    except Exception as e:
        model_lock.release()
        print(e)
        message = dumps({"error": "Smiles failed to parse"})
        abort(Response(message, 406))

    if data is None:
        return abort(400)

    print(data)
    return data
