"""Tests for structural re-ranking of kd-tree candidates."""

import os
import sys

from rdkit import Chem, RDLogger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from python_web_server import rerank

RDLogger.DisableLog("rdApp.*")

ASPIRIN = "CC(=O)Oc1ccccc1C(=O)O"
SALICYLIC = "O=C(O)c1ccccc1O"
PROPIONATE_ESTER = "CCC(=O)Oc1ccccc1C(=O)O"
CAFFEINE = "Cn1c(=O)c2c(ncn2C)n(C)c1=O"
HEXANE = "CCCCCC"


def hit(smiles, distance, identifier="id"):
    return {"smiles": smiles, "distance": distance, "identifier": identifier}


def test_returns_requested_count():
    hits = [hit(s, 0.1) for s in (ASPIRIN, SALICYLIC, CAFFEINE, HEXANE)]
    assert len(rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 2)) == 2


def test_returns_all_when_fewer_than_requested():
    hits = [hit(ASPIRIN, 0.1), hit(CAFFEINE, 0.2)]
    assert len(rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 10)) == 2


def test_empty_candidate_list():
    assert rerank.rerank(Chem.MolFromSmiles(ASPIRIN), [], 5) == []


def test_sorted_by_descending_similarity():
    hits = [hit(s, 0.1) for s in (HEXANE, CAFFEINE, SALICYLIC, ASPIRIN)]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 4)

    sims = [h["similarity"] for h in ranked]
    assert sims == sorted(sims, reverse=True)


def test_identical_molecule_ranks_first_regardless_of_input_order():
    """The whole point: the kd-tree's order must not survive re-ranking."""

    hits = [hit(HEXANE, 0.01), hit(CAFFEINE, 0.02), hit(ASPIRIN, 0.99)]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 3)

    assert ranked[0]["smiles"] == ASPIRIN
    assert abs(ranked[0]["similarity"] - 1.0) < 1e-6
    assert ranked[-1]["smiles"] == HEXANE


def test_close_analog_outranks_unrelated_molecule():
    hits = [hit(HEXANE, 0.01), hit(PROPIONATE_ESTER, 0.98)]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 2)

    assert ranked[0]["smiles"] == PROPIONATE_ESTER


def test_unparseable_smiles_are_dropped():
    hits = [hit("not a molecule", 0.01), hit(ASPIRIN, 0.5)]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 5)

    assert [h["smiles"] for h in ranked] == [ASPIRIN]


def test_falls_back_to_kdtree_order_when_nothing_parses():
    """A query should still answer rather than fail on bad database rows."""

    hits = [hit("???", 0.01), hit("!!!", 0.02)]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 1)

    assert ranked == [hits[0]]


def test_original_hit_fields_are_preserved():
    hits = [hit(ASPIRIN, 0.25, identifier="Z123")]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 1)

    assert ranked[0]["identifier"] == "Z123"
    assert ranked[0]["distance"] == 0.25


def test_input_hits_are_not_mutated():
    hits = [hit(ASPIRIN, 0.25)]
    rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 1)

    assert "similarity" not in hits[0]


def test_fingerprint_width_matches_configuration():
    fp = rerank.fingerprint(Chem.MolFromSmiles(ASPIRIN))
    assert fp.GetNumBits() == rerank.FP_BITS


def test_threaded_parse_matches_in_process_parse():
    """The thread pool reorders internally; order must be restored exactly."""

    smiles = [ASPIRIN, SALICYLIC, "bad", CAFFEINE, HEXANE] * 40
    threaded = rerank.parse_smiles(smiles)
    assert len(threaded) == len(smiles)

    for s, mol in zip(smiles, threaded):
        expected = Chem.MolFromSmiles(s)
        if expected is None:
            assert mol is None
        else:
            assert Chem.MolToSmiles(mol) == Chem.MolToSmiles(expected)


def test_small_batches_bypass_the_thread_pool():
    mols = rerank.parse_smiles([ASPIRIN, "bad"])
    assert Chem.MolToSmiles(mols[0]) == Chem.MolToSmiles(Chem.MolFromSmiles(ASPIRIN))
    assert mols[1] is None


def test_scales_to_a_full_retrieval_batch():
    """MAX_NEIGHBORS-sized input is the normal case, not an edge case."""

    hits = [hit(s, i * 0.001) for i, s in
            enumerate([ASPIRIN, SALICYLIC, CAFFEINE, HEXANE] * 256)]
    ranked = rerank.rerank(Chem.MolFromSmiles(ASPIRIN), hits, 8)

    assert len(ranked) == 8
    assert all(h["smiles"] == ASPIRIN for h in ranked)


if __name__ == "__main__":
    # Runnable without pytest, which is not installed in the project venv.
    failures = 0
    for name, fn in sorted(list(globals().items())):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  pass  {name}")
        except AssertionError as e:
            failures += 1
            print(f"  FAIL  {name}: {e}")

    print(f"\n{failures} failure(s)")
    sys.exit(1 if failures else 0)
