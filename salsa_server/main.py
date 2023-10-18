import sys
sys.path.append(".")
from flask import Flask, render_template, request, abort, Response, jsonify
import threading

from json import dumps

from modeling.model import SalsaONNXModel

app = Flask(__name__)

model = SalsaONNXModel("modeling/models/salsa_bigmat.onnx")
model_lock = threading.Lock()

@app.route('/')
def home():
    print("main")

@app.route('/smiles', methods=['GET', 'POST'])
def smiles():

    '''
    print(dir(request))
    print(request.form)
    print(request.values)
    print(request.json)
    '''

    print(request.args)
    args = list(request.args)
    try:
        assert(len(args) == 1)
    except:
        return "Invalid input"

    smiles = args[0]


    data = None
    try:
        model_lock.acquire()
        data = model.embed(smiles)
        model_lock.release()
    except:
        model_lock.release()
        message = dumps({"error": "Smiles failed to parse"})
        abort(Response(message, 406))

    if data is None:
        return abort(400)

    print(data)
    return data
