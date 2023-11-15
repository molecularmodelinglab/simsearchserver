import glob
from tqdm import tqdm

filenames = list(glob.glob("/data/smallsa_from_paper/*clean"))

for filename in tqdm(filenames):

    f = open(filename, 'r')

    output_filename = filename + "_trimmed"

    out_file = open(output_filename, 'w')

    out_file.write("smiles,id_val,embedding...\n")

    for i, line in tqdm(enumerate(f)):

        if i == 0:
            continue

        s = line.split(",")

        smiles = s[3]
        id_val = s[5]
        embedding = s[6]

        id_val = id_val.replace("____", "_")

        embedding = embedding.replace("[","").replace("]", "").replace('"', '').strip()
        embedding_vec = embedding.split()

        try:
            embedding_vec = [float(x) for x in embedding_vec]
        except:
            print(f"FAILURE: {line}")
        
        out_s = f"{smiles},{id_val}"
        for thing in embedding_vec:
            out_s = out_s + "," + f"{thing:.7f}"

        out_s += '\n'

        out_file.write(out_s)
