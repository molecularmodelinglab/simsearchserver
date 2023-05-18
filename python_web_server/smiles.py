from python_web_server.smallsa.utils import do_query
from rdkit.Chem import MolFromSmiles, MolToSmiles, AllChem, DataStructs
from rdkit.Chem import Draw
import pickle
import numpy as np
from python_web_server import rust_query
from python_web_server import embedding
import mols2grid

embed_map = {
        'morgan_pca_16': embedding.embed_morgan_pca_16,
        'smallsa_16': embedding.embed_smallsa_16,
        'smallsa_8': embedding.embed_smallsa_8,
        }

query_map = {
        'morgan_pca_16': rust_query.query_morgan_pca_16,
        'smallsa_16': rust_query.query_smallsa_16,
        'smallsa_8': rust_query.query_smallsa_8,
        }


def get_molecule_data_from_smiles(smiles, drawn=None, options=None):
    if (len(smiles.strip()) == 0) and (len(drawn.strip()) == 0):
        return None

    mol = MolFromSmiles(str(drawn))
    _smiles = drawn
    if mol is None or len(_smiles.strip()) == 0:
        mol = MolFromSmiles(str(smiles))
        _smiles = smiles
    if mol is None or len(_smiles.strip()) == 0:
        return None

    _res, _dist_from_norm = do_query(MolToSmiles(mol, isomericSmiles=False))

    output_html = mols2grid.display(_res, subset=["mols2grid-id", "img", "Distance"], tooltip=["SMILES", "Distance"])._repr_html_()

    return {
        'o_dist': _dist_from_norm,
        'svg': Draw.MolsToGridImage([mol], useSVG=True, molsPerRow=1),
        'SMILES': smiles,
        'grid_html': output_html
    }

def get_smiles_from_id(id_list):

    from mysql import connector
    #cnx = connector.connect(user='josh', host='localhost',database='chembl_2mil', allow_local_infile = True, use_pure = True)
    cnx = connector.connect(user='josh', host='localhost',database='chembl_2mil', allow_local_infile = True, use_pure = False)
    cursor = cnx.cursor()

    d = {}
    for id_val in id_list:
        query = f'SELECT * FROM structures where id="{id_val}";'

        cursor.execute(query)
        for hit in cursor:
            id_val = hit[0]
            smiles = hit[1]
            d[id_val] = smiles

        if id_val not in d:
            d[id_val] = None

    return d



def query_smiles(method, smiles, drawn=None, options=None):

    try:
        query_func = query_map[method]
        embed_func = embed_map[method]
    except:
        raise Exception(f"Method {method} not recognized")

    if (len(smiles.strip()) == 0) and (len(drawn.strip()) == 0):
        return None

    mol = MolFromSmiles(smiles)

    embedding = embed_func(mol)
    result = query_func(embedding)

    
    hits = result['hits']
    id_values = []
    distances = []
    for hit,info in hits.items():
        print(hit)
        print(info["distance"])
        id_values.append(hit.replace('\x00',''))
        distances.append(info["distance"])

    d = get_smiles_from_id(id_values)

    from rdkit import Chem
    smiles_list = []
    mols = []
    for id_val in id_values:
        smiles_list.append(d[id_val])
        #mol = Chem.MolFromSmiles(d[id_val])
        #mols.append(mol)


    import pandas as pd
    #res =  pd.DataFrame({"SMILES": smiles.squeeze(), "Distance": np.round(dists.squeeze(), 6)}), dist_from_origin
    res =  pd.DataFrame({"SMILES": smiles_list, "Distance": distances})

    output_html = mols2grid.display(res, subset=["mols2grid-id", "img", "Distance"], tooltip=["SMILES", "Distance"])._repr_html_()

    return {
        #'o_dist': _dist_from_norm,
        'svg': Draw.MolsToGridImage([mol], useSVG=True, molsPerRow=1),
        'SMILES': smiles,
        'grid_html': output_html
    }

