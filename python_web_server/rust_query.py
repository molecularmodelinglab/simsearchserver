import os

import requests

KDTREE_URL = os.environ.get("KDTREE_URL", "http://127.0.0.1:3000")
# A power of two, because SALSA is not chirality-aware: the 2^n stereoisomers
# of a molecule with n chiral centers are ties in the embedding space, split
# only by the tie-breaking noise. k=8 returns a full tie group for up to three
# chiral centers.
NUM_NEIGHBORS = int(os.environ.get("NUM_NEIGHBORS", "8"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))


def query_descriptor(embedding, num_neighbors=None):

    k = num_neighbors or NUM_NEIGHBORS

    response = requests.post(
        f"{KDTREE_URL}/nn/{k}",
        json={"descriptor": list(embedding)},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    return response.json()


def index_info():

    response = requests.get(f"{KDTREE_URL}/health", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    return response.json()
