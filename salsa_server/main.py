import sys
sys.path.append(".")
from flask import Flask, render_template, request, abort, Response, jsonify
import threading

from json import dumps

from modeling.model import SalsaONNXModel, MorganPCAModel

app = Flask(__name__)

salsa16_model = SalsaONNXModel("modeling/models/salsa16.onnx")
salsa8_model = SalsaONNXModel("modeling/models/salsa8.onnx")
morgan_model = MorganPCAModel()
model_lock = threading.Lock()

@app.route('/')
def home():
    print("main")

@app.route('/smiles/<method>/<smiles_value>', methods=['GET', 'POST'])
def smiles(smiles_value, method):

    if method.lower() == "salsa16":
        model = salsa16_model
    elif method.lower() == "salsa8":
        model = salsa8_model
    elif method.lower() == "morganpca16":
        model = morgan_model
    else:
        message = dumps({"error": f"Embedding method not recognized: {method}"})
        abort(Response(message, 406))


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

