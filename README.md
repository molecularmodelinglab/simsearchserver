To get the server running, there are three components:

1. The KD Tree server ('rust_server') that takes in a descriptor for a nearest neighbor search
2. The smiles lookup server (not in this repo) that returns the SMILES for a given molecule ID
3. The Flask server ('python_web_server') that handles all of the frontend

TODO:
- [ ] get salsa embeddings working
- [ ] get salsa and morgan on the ~40mil enamine diversity set
- [ ] the big 40bil run
