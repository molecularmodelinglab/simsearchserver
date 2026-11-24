"""Client for the kd-tree nearest-neighbor service."""

import os

import requests

KDTREE_URL = os.environ.get("KDTREE_URL", "http://127.0.0.1:3000")

# how many candidates the kd-tree returns
MAX_NEIGHBORS = int(os.environ.get("MAX_NEIGHBORS", "1024"))
# how many of those survive re-ranking and reach the user
NUM_NEIGHBORS = int(os.environ.get("NUM_NEIGHBORS", "8"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))


def query_descriptor(embedding, max_neighbors=None):
    """Nearest neighbors of `embedding`, in index order, before re-ranking."""

    k = max_neighbors or MAX_NEIGHBORS

    response = requests.post(
        f"{KDTREE_URL}/nn/{k}",
        json={"descriptor": list(embedding)},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    return response.json()


def index_info():
    """Record count and descriptor length of the index being served."""

    response = requests.get(f"{KDTREE_URL}/health", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    return response.json()
