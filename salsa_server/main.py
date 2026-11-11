import sys
sys.path.append(".")
from flask import Flask, render_template, request, abort, Response, jsonify
import threading

from json import dumps

from modeling.model import SalsaModel

app = Flask(__name__)

salsa_model = SalsaModel()
model_lock = threading.Lock()

print(f"Loaded salsa model from {salsa_model.path}, embedding dimension {salsa_model.dim}")


@app.route('/')
def home():
    print("main")


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "dim": salsa_model.dim, "model": salsa_model.path})


@app.route('/smiles/<method>', methods=['GET', 'POST'])
def smiles(method):

    if method.lower() != "salsa16":
        message = dumps({"error": f"Embedding method not recognized: {method}"})
        abort(Response(message, 406))

    payload = request.get_json(silent=True) or {}
    smiles_value = payload.get("smiles") or request.args.get("smiles")

    if not smiles_value:
        message = dumps({"error": "No SMILES supplied; send {\"smiles\": ...} or ?smiles="})
        abort(Response(message, 400))

    try:
        with model_lock:
            data = salsa_model.embed(smiles_value)
    except Exception as e:
        print(e)
        message = dumps({"error": f"Could not embed SMILES: {e}"})
        abort(Response(message, 406))

    return jsonify(data)
