from flask import Flask, render_template, request, abort, jsonify
from python_web_server.smiles import query_smiles

app = Flask(__name__)

METHOD = "salsa16"


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/models', methods=['GET'])
def mol_properties():
    return jsonify(["ChEMBL", "Enamine"]), 200


@app.route('/search', methods=['GET', 'POST'])
def smiles():

    payload = request.get_json(silent=True) or {}

    smiles = payload.get('q') or request.args.get('q')
    options = payload.get('options')

    if not smiles:
        return abort(400)

    data = query_smiles(METHOD, smiles, options=options)

    if data is None:
        return abort(400)

    return data
