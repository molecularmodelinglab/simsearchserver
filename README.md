To get the server running, there are three components:

1. The KD Tree server (`rust_server`) that takes in a SMILES string and return nearest neighbors
2. The SALSA server (`salsa_server`) that takes in a SMILES string and returns an embedding
3. The Flask server (`python_web_server`) that is a user interaface for the `rust_server`
