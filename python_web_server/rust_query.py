import os

import requests

KDTREE_URL = os.environ.get("KDTREE_URL", "http://127.0.0.1:3000")
NUM_NEIGHBORS = int(os.environ.get("NUM_NEIGHBORS", "100"))
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
