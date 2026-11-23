import os

from rdkit.Chem import MolFromSmiles, Draw
import mols2grid
import pandas as pd
import requests

from python_web_server import rerank, rust_query

SALSA_URL = os.environ.get("SALSA_URL", "http://127.0.0.1:5000")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))


def embed_smiles(smiles, method="salsa16"):

    response = requests.post(
        f"{SALSA_URL}/smiles/{method}",
        json={"smiles": smiles},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    return response.json()


def query_smiles(method, smiles, drawn=None, options=None):

    smiles = (smiles or "").strip()

    if not smiles:
        return None

    mol = MolFromSmiles(smiles)

    if mol is None:
        return None

    embedding = embed_smiles(smiles, method)

    result = rust_query.query_descriptor(embedding)

    # Retrieve wide, rank well, show few.
    hits = rerank.rerank(mol, result["hits"], rust_query.NUM_NEIGHBORS)

    frame = pd.DataFrame(
        {
            "SMILES": [hit["smiles"] for hit in hits],
            "ID": [hit["identifier"] for hit in hits],
            "Similarity": [round(hit.get("similarity", float("nan")), 3) for hit in hits],
            "Distance": [round(hit["distance"], 3) for hit in hits],
        }
    )

    grid_html = mols2grid.display(
        frame,
        subset=["mols2grid-id", "img", "Similarity"],
        tooltip=["SMILES", "ID", "Similarity", "Distance"],
    )._repr_html_()

    return {
        "svg": Draw.MolsToGridImage([mol], useSVG=True, molsPerRow=1),
        "SMILES": smiles,
        "grid_html": grid_html,
    }
