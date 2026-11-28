"""End-to-end serving: build a small index with fingerprints, serve it, query it.

Covers the seam the unit tests cannot reach — the builder writing a fingerprint
per added record, and the server ranking against that column.
"""

import http.client
import json
import os
import socket
import subprocess
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simsearch import config as cfg
from simsearch import stages

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIM = 10

SMILES = [
    "CCO", "CCN", "CCC", "c1ccccc1", "c1ccccc1O", "c1ccccc1N", "CC(=O)O",
    "CC(=O)N", "CC(C)O", "CC(C)N", "CCOCC", "CCOC(C)=O", "c1ccc(cc1)C(=O)O",
    "c1ccc(cc1)C(=O)N", "c1ccc(cc1)CCN", "C1CCCCC1", "C1CCNCC1", "C1CCOCC1",
    "CN1CCCC1", "CC1=CC=CC=C1", "CC1=CC=C(C=C1)O", "CC1=CC=C(C=C1)N",
    "CCCCO", "CCCCN", "CCCCCC", "c1ccc2ccccc2c1", "c1ccc2[nH]ccc2c1",
    "c1cnc2ccccc2c1", "CC(=O)Nc1ccccc1", "CC(=O)Oc1ccccc1",
    "CSc1ccccc1", "CSCCN", "FC(F)(F)c1ccccc1", "Clc1ccccc1", "Brc1ccccc1",
    "OCC1OC(O)C(O)C1O", "NCC(=O)O", "CC(N)C(=O)O", "CSCCC(N)C(=O)O",
    "c1ccc(cc1)S(=O)(=O)N", "CN(C)CCN", "CCN(CC)CC", "O=C1CCCN1",
    "O=C1CCCCN1", "c1ccc(cc1)Oc1ccccc1", "CC(=O)CC(=O)C",
    "c1ccc(cc1)C#N", "CC#N", "C=CC(=O)O", "C=CC(=O)N",
]


def binary(name):
    for profile in ("release", "debug"):
        path = os.path.join(REPO, "rust_server", "target", profile, name)
        if os.path.exists(path):
            return path

    return None


needs_binaries = pytest.mark.skipif(
    binary("builder") is None or binary("server") is None,
    reason="run `cargo build` in rust_server first")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def tanimoto(left, right):
    shared = np.unpackbits(left & right).sum()
    union = np.unpackbits(left | right).sum()

    return 0.0 if union == 0 else shared / union


@pytest.fixture(scope="module")
def served(tmp_path_factory):
    """A built, fingerprinted index with a server in front of it."""

    root = tmp_path_factory.mktemp("serving")
    projection = root / "prepared" / "projection"
    projection.mkdir(parents=True)

    rng = np.random.default_rng(20260101)
    arrays = [pa.array([f"CMPD{i:04d}" for i in range(len(SMILES))]),
              pa.array(SMILES)]
    names = ["ID", "SMILES"]
    for i in range(DIM):
        arrays.append(pa.array(rng.normal(size=len(SMILES)).astype(np.float32)))
        names.append(f"dim_{i}")

    pq.write_table(pa.table(arrays, names=names), projection / "shard_00.parquet")

    config_path = root / "config.yaml"
    config_path.write_text(yaml.safe_dump({
        "output": str(root / "prepared"),
        "corpus": {"input": str(projection)},
        "projection": {"method": "pca", "dim": DIM},
        "index": {"directory": str(root / "index")},
    }))
    config = cfg.load(str(config_path))

    # The real stage, so the stored column is the shipping spec.
    stages.fingerprint(config, processes=1)

    index_config = root / "index.yaml"
    index_config.write_text(
        f'directory: "{root / "index"}"\n'
        f"desc_length: {DIM}\n"
        "record_page_length: 4096\n")

    subprocess.run([
        binary("builder"), "build-from-files",
        "--filenames", str(projection / "shard_00.parquet"),
        "--config-filename", str(index_config),
        "--fingerprints", str(root / "prepared" / "fingerprint"),
    ], check=True, capture_output=True)

    port = free_port()
    proc = subprocess.Popen(
        [binary("server"), "--dirname", str(root / "index"), "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    deadline = time.time() + 120
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited: {proc.stdout.read()}")
        try:
            post(port, "/health", None)
            break
        except Exception:
            time.sleep(0.2)
    else:
        proc.terminate()
        raise TimeoutError("server never became ready")

    stored = np.fromfile(root / "prepared" / "fingerprint" / "shard_00.fp",
                         dtype=np.uint8).reshape(len(SMILES), -1)

    yield {"port": port, "root": root, "stored": stored, "table": pa.table(arrays, names=names)}

    proc.terminate()
    proc.wait(timeout=60)


def post(port, path, payload):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)

    if payload is None:
        conn.request("GET", path)
    else:
        conn.request("POST", path, body=json.dumps(payload),
                     headers={"Content-Type": "application/json"})

    response = conn.getresponse()
    body = response.read()

    if response.status != 200:
        raise RuntimeError(f"{path} returned {response.status}: {body[:200]}")

    return json.loads(body)


def query_vector(table, row):
    return [float(table[f"dim_{i}"][row].as_py()) for i in range(DIM)]


@needs_binaries
def test_health_reports_the_fingerprint_column(served):
    health = post(served["port"], "/health", None)

    assert health["num_records"] == len(SMILES)
    assert health["desc_length"] == DIM
    assert health["fingerprint_bytes"] == 64


@needs_binaries
def test_a_record_is_its_own_nearest_neighbor(served):
    table = served["table"]
    result = post(served["port"], "/nn/5", {"descriptor": query_vector(table, 7)})

    assert result["hits"][0]["identifier"] == "CMPD0007"
    assert result["hits"][0]["distance"] == pytest.approx(0.0, abs=1e-6)
    assert result["hits"][0]["smiles"] == SMILES[7]


@needs_binaries
def test_hits_come_back_in_ascending_distance(served):
    table = served["table"]
    result = post(served["port"], "/nn/20", {"descriptor": query_vector(table, 3)})

    distances = [hit["distance"] for hit in result["hits"]]
    assert distances == sorted(distances)


@needs_binaries
def test_identifiers_are_not_prefixed(served):
    table = served["table"]
    result = post(served["port"], "/nn/3", {"descriptor": query_vector(table, 11)})

    assert result["hits"][0]["identifier"] == "CMPD0011"


@needs_binaries
def test_reranking_matches_tanimoto_over_the_stored_column(served):
    table, stored = served["table"], served["stored"]

    for row in (0, 13, 31):
        query = stored[row]

        result = post(served["port"], "/nn/40", {
            "descriptor": query_vector(table, row),
            "fingerprint": [int(b) for b in query],
            "num_neighbors": 6,
        })

        assert len(result["hits"]) == 6

        similarities = [hit["similarity"] for hit in result["hits"]]
        assert similarities == sorted(similarities, reverse=True)
        assert similarities[0] == pytest.approx(1.0)

        for hit in result["hits"]:
            index = int(hit["identifier"].removeprefix("CMPD"))
            assert hit["similarity"] == pytest.approx(
                tanimoto(query, stored[index]), abs=1e-5)


@needs_binaries
def test_similarity_is_absent_without_a_query_fingerprint(served):
    table = served["table"]
    result = post(served["port"], "/nn/5", {"descriptor": query_vector(table, 2)})

    assert all("similarity" not in hit for hit in result["hits"])


@needs_binaries
def test_a_wrong_width_fingerprint_is_refused(served):
    table = served["table"]

    with pytest.raises(RuntimeError, match="400"):
        post(served["port"], "/nn/5", {
            "descriptor": query_vector(table, 1),
            "fingerprint": [0] * 32,
        })


@needs_binaries
def test_a_wrong_dimension_descriptor_is_refused(served):
    with pytest.raises(RuntimeError, match="400"):
        post(served["port"], "/nn/5", {"descriptor": [0.0] * (DIM + 2)})


@needs_binaries
def test_an_index_without_a_projection_serves_no_transform(served):
    with pytest.raises(RuntimeError, match="404"):
        post(served["port"], "/transform", None)


@needs_binaries
def test_a_batch_matches_the_same_queries_run_one_at_a_time(served):
    table, stored = served["table"], served["stored"]
    rows = [0, 7, 13, 31, 44]

    one_at_a_time = [
        post(served["port"], "/nn/40", {
            "descriptor": query_vector(table, row),
            "fingerprint": [int(b) for b in stored[row]],
            "num_neighbors": 5,
        })
        for row in rows
    ]

    batch = post(served["port"], "/nn/batch/40", {
        "queries": [{"descriptor": query_vector(table, row),
                     "fingerprint": [int(b) for b in stored[row]]}
                    for row in rows],
        "num_neighbors": 5,
    })

    assert batch["num_queries"] == len(rows)
    assert len(batch["results"]) == len(rows)

    for single, batched in zip(one_at_a_time, batch["results"]):
        assert [hit["identifier"] for hit in batched["hits"]] == \
               [hit["identifier"] for hit in single["hits"]]
        assert [hit["similarity"] for hit in batched["hits"]] == \
               [hit["similarity"] for hit in single["hits"]]


@needs_binaries
def test_batch_results_stay_in_the_order_submitted(served):
    table = served["table"]
    rows = [31, 2, 19, 7]

    batch = post(served["port"], "/nn/batch/5", {
        "queries": [{"descriptor": query_vector(table, row)} for row in rows],
    })

    for row, result in zip(rows, batch["results"]):
        assert result["hits"][0]["identifier"] == f"CMPD{row:04d}"


@needs_binaries
def test_an_empty_batch_is_refused(served):
    with pytest.raises(RuntimeError, match="400"):
        post(served["port"], "/nn/batch/5", {"queries": []})


@needs_binaries
def test_one_bad_descriptor_refuses_the_whole_batch(served):
    table = served["table"]

    with pytest.raises(RuntimeError, match="400"):
        post(served["port"], "/nn/batch/5", {"queries": [
            {"descriptor": query_vector(table, 1)},
            {"descriptor": [0.0] * (DIM + 2)},
        ]})
