"""HTTP service that turns SMILES into SALSA embeddings."""

import threading
from json import dumps

from flask import Flask, Response, abort, jsonify, request

from salsa_server.modeling.model import SalsaModel

app = Flask(__name__)

salsa_model = SalsaModel()
model_lock = threading.Lock()

print(f"Loaded salsa model {salsa_model.model_name}, embedding dimension {salsa_model.dim}")


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "dim": salsa_model.dim, "model": salsa_model.model_name})


@app.route('/smiles/<method>', methods=['GET', 'POST'])
def smiles(method):

    if method.lower() != salsa_model.model_name.lower():
        message = dumps({"error": f"This server serves {salsa_model.model_name}, not {method}"})
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
