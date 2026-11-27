"""Receive-time preprocessing, deduplication and CSV rendering."""

import asyncio
import csv
import io
import os
import sys

import numpy as np
import pytest
from fastapi import HTTPException
from rdkit.Chem import rdFingerprintGenerator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simsearch_api import batch as batching
from simsearch_api.app import _as_csv
from simsearch_api.chemistry import FINGERPRINT_BITS, Chemistry


@pytest.fixture(scope="module")
def chemistry():
    """A Chemistry with everything `prepare` uses, and no model."""

    stub = Chemistry.__new__(Chemistry)
    stub.max_smiles_length = 150
    stub._generator = rdFingerprintGenerator.GetAtomPairGenerator(
        fpSize=FINGERPRINT_BITS, countSimulation=False)

    return stub


def test_stereoisomers_collapse_to_one_embedding(chemistry):
    result = batching.prepare(
        ["C[C@H](N)C(=O)O", "C[C@@H](N)C(=O)O", "CC(N)C(=O)O"], chemistry)

    assert result.counts == {"submitted": 3, "unique": 1, "searched": 3, "failed": 0}
    assert len(set(result.assignment.values())) == 1


def test_the_shown_form_keeps_stereo_and_the_dedup_key_drops_it(chemistry):
    prepared = chemistry.prepare("C[C@H](N)C(=O)O")

    assert "@" in prepared.canonical
    assert "@" not in prepared.key


def test_stereoisomers_are_shown_differently_though_they_dedup_together(chemistry):
    result = batching.prepare(["C[C@H](N)C(=O)O", "C[C@@H](N)C(=O)O"], chemistry)

    assert result.counts["unique"] == 1
    shown = {row.prepared.canonical for row in result.rows}
    assert len(shown) == 2


def test_distinct_molecules_stay_distinct(chemistry):
    result = batching.prepare(["CCO", "CCN", "CCC"], chemistry)

    assert result.counts["unique"] == 3


def test_the_same_molecule_written_two_ways_is_one(chemistry):
    result = batching.prepare(["CCO", "OCC"], chemistry)

    assert result.counts["unique"] == 1


def test_a_salt_is_reduced_to_its_largest_fragment(chemistry):
    result = batching.prepare(["CC(=O)[O-].[Na+]"], chemistry)
    row = result.rows[0]

    assert row.ok
    assert "largest fragment" in row.prepared.note
    assert "Na" not in row.prepared.canonical


def test_one_bad_row_does_not_sink_the_others(chemistry):
    result = batching.prepare(["CCO", "not-a-smiles", "CCN"], chemistry)

    assert [row.ok for row in result.rows] == [True, False, True]
    assert "could not parse" in result.rows[1].error
    assert result.counts == {"submitted": 3, "unique": 2, "searched": 2, "failed": 1}


def test_blank_rows_are_reported_not_dropped(chemistry):
    result = batching.prepare(["CCO", "", "   "], chemistry)

    assert len(result.rows) == 3
    assert result.counts["failed"] == 2
    assert [row.index for row in result.rows] == [0, 1, 2]


def test_an_over_length_smiles_fails_its_own_row(chemistry):
    result = batching.prepare(["C" * 200, "CCO"], chemistry)

    assert "151 characters" in result.rows[0].error or "200 characters" in result.rows[0].error
    assert result.rows[1].ok


def test_fingerprints_are_the_shipping_width(chemistry):
    result = batching.prepare(["c1ccccc1O"], chemistry)

    assert len(result.rows[0].prepared.fingerprint) == FINGERPRINT_BITS // 8


class FakeLibrary:
    """Returns each query's own descriptor as its single hit."""

    name = "fake"
    num_neighbors = 2
    fingerprint_bytes = 64

    def __init__(self):
        self.batches = []

    def project(self, embeddings):
        return embeddings

    async def search_batch(self, _client, descriptors, fingerprints, wanted, _timeout):
        self.batches.append(len(descriptors))

        return {"results": [
            {"hits": [{"identifier": f"HIT{int(d[0])}", "smiles": "CCO",
                       "distance": 0.0, "similarity": 1.0}] * wanted}
            for d in descriptors
        ]}


class FakeChemistry:
    """Embeds each SMILES as its position, so hits name their own query."""

    def __init__(self, real):
        self._real = real
        self.calls = []

    max_smiles_length = 150

    def prepare(self, smiles):
        return self._real.prepare(smiles)

    def embed_many(self, smiles):
        self.calls.append(list(smiles))

        return np.array([[float(i)] for i in range(len(smiles))], dtype=np.float32)


@pytest.fixture
def served(chemistry, monkeypatch):
    from simsearch_api import app as api

    library = FakeLibrary()
    fake = FakeChemistry(chemistry)

    monkeypatch.setattr(api.state, "config", {
        "limits": {"max_smiles_length": 150, "max_num_neighbors": 96,
                   "max_batch_size": 10, "batch_concurrency": 1,
                   "batch_timeout_seconds": 60.0}})
    monkeypatch.setattr(api.state, "chemistry", fake)
    monkeypatch.setattr(api.state, "libraries", {"fake": library})
    monkeypatch.setattr(api.state, "client", None)
    monkeypatch.setattr(api.state, "batch_gate", asyncio.Semaphore(1))

    return {"api": api, "library": library, "chemistry": fake}


def run(coroutine):
    return asyncio.run(coroutine)


def test_duplicates_are_embedded_once_but_answered_twice(served):
    api = served["api"]

    answer = run(api._search_batch(api.BatchBody(
        smiles=["CCO", "OCC", "CCN"], library="fake")))

    assert served["chemistry"].calls == [["CCO", "CCN"]]
    assert served["library"].batches == [2]
    assert answer["counts"] == {"submitted": 3, "unique": 2, "searched": 3, "failed": 0}

    identifiers = [row["hits"][0]["identifier"] for row in answer["results"]]
    assert identifiers == ["HIT0", "HIT0", "HIT1"]


def test_failed_rows_keep_every_other_row_aligned(served):
    api = served["api"]

    answer = run(api._search_batch(api.BatchBody(
        smiles=["CCO", "not-a-smiles", "CCN", "", "CCC"], library="fake")))

    statuses = [row["status"] for row in answer["results"]]
    assert statuses == ["ok", "error", "ok", "error", "ok"]

    found = [row["hits"][0]["identifier"] for row in answer["results"] if row["status"] == "ok"]
    assert found == ["HIT0", "HIT1", "HIT2"]
    assert served["chemistry"].calls == [["CCO", "CCN", "CCC"]]


def test_a_note_survives_into_the_result(served):
    api = served["api"]

    answer = run(api._search_batch(api.BatchBody(
        smiles=["CC(=O)[O-].[Na+]"], library="fake")))

    assert "largest fragment" in answer["results"][0]["note"]


def test_over_the_batch_limit_is_refused(served):
    api = served["api"]

    with pytest.raises(HTTPException) as raised:
        run(api._search_batch(api.BatchBody(smiles=["CCO"] * 11, library="fake")))

    assert raised.value.status_code == 400


def test_an_empty_list_is_refused(served):
    api = served["api"]

    with pytest.raises(HTTPException) as raised:
        run(api._search_batch(api.BatchBody(smiles=[], library="fake")))

    assert raised.value.status_code == 400


def test_a_second_batch_is_turned_away_while_one_runs(served):
    api = served["api"]

    async def both():
        await api.state.batch_gate.acquire()
        try:
            return await api._search_batch(api.BatchBody(smiles=["CCO"], library="fake"))
        finally:
            api.state.batch_gate.release()

    with pytest.raises(HTTPException) as raised:
        run(both())

    assert raised.value.status_code == 429


def test_every_row_fails_without_calling_the_index(served):
    api = served["api"]

    answer = run(api._search_batch(api.BatchBody(
        smiles=["not-a-smiles", ""], library="fake")))

    assert answer["counts"]["searched"] == 0
    assert served["library"].batches == []


def test_csv_has_one_row_per_hit_and_one_per_failure():
    answer = {"results": [
        {"query": "CCO", "status": "ok", "canonical": "CCO", "hits": [
            {"identifier": "A1", "smiles": "CCO", "distance": 0.0, "similarity": 1.0},
            {"identifier": "A2", "smiles": "CCN", "distance": 0.5, "similarity": 0.4},
        ]},
        {"query": "bad", "status": "error", "error": "could not parse"},
    ]}

    rows = list(csv.DictReader(io.StringIO(_as_csv(answer))))

    assert len(rows) == 3
    assert [row["rank"] for row in rows] == ["1", "2", ""]
    assert rows[2]["error"] == "could not parse"
    assert rows[0]["identifier"] == "A1"
