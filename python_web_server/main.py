from flask import Flask, render_template, request, abort, Response, jsonify
from python_web_server.smiles import query_smiles

app = Flask(__name__)


@app.route('/')
def home():
    print("main")
    return render_template('index.html')


@app.route('/models', methods=['GET'])
def mol_properties():
    print("database_selection")
    return jsonify(["ChEMBL", "Enamine"]), 200


@app.route('/search', methods=['GET', 'POST'])
def smiles():
    print(dir(request))
    print(request.form)
    print(request.values)
    print(request.json)

    print(request.args)

    #OVERRIDE
    method = "smallsa_8"
    drawn = False
    options = None
    #OVERRIDE

    print(request.json.get('option', None))
    smiles = request.json.get('q', None)
    print(smiles)
    print("HERE")
    data = query_smiles(method, smiles, drawn, options)

    if data is None:
        return abort(400)

    return data
