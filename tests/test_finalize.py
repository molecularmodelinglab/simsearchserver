"""Tests for the resolved manifest written into a built index."""

import os
import struct
import sys

import numpy as np
import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simsearch import config as cfg
from simsearch import finalize as fin


def build_fake_index(directory, records, desc_length=10, page=16384):
    """Enough of a built index for the manifest to be derivable."""

    os.makedirs(directory, exist_ok=True)

    with open(os.path.join(directory, "config.yaml"), "w") as handle:
        yaml.safe_dump({
            "directory": str(directory),
            "desc_length": desc_length,
            "record_page_length": page,
            "format_version": 1,
        }, handle)

    header = bytearray(24)
    header[:8] = b"KDTDBASE"
    struct.pack_into("<I", header, 8, 1)
    struct.pack_into("<Q", header, 16, records)
    with open(os.path.join(directory, "db.db"), "wb") as handle:
        handle.write(header)


def prepared_corpus(root, rows=120, source_dim=16, dim=10):
    """A projection stage with a fitted basis and one shard."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    projection = os.path.join(root, "projection")
    os.makedirs(projection, exist_ok=True)

    rng = np.random.default_rng(0)
    arrays = [pa.array([f"ID{i}" for i in range(rows)]),
              pa.array(["CCO"] * rows)]
    names = ["ID", "SMILES"]
    for i in range(dim):
        arrays.append(pa.array(rng.normal(size=rows).astype(np.float32)))
        names.append(f"dim_{i}")
    pq.write_table(pa.table(arrays, names=names),
                   os.path.join(projection, "shard_00.parquet"))

    np.savez(os.path.join(projection, "pca.npz"),
             mean=rng.normal(size=source_dim),
             components=np.linalg.qr(rng.normal(size=(source_dim, source_dim)))[0],
             variances=np.arange(source_dim)[::-1].astype(float))

    return projection


def make_config(tmp_path, index_directory, dim=10):
    data = {
        "output": str(tmp_path / "prepared"),
        "corpus": {"input": str(tmp_path / "corpus")},
        "projection": {"method": "pca", "dim": dim},
        "index": {"directory": str(index_directory)},
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))

    return cfg.load(str(path))


def test_record_count_comes_from_the_database_header(tmp_path):
    index = tmp_path / "index"
    build_fake_index(index, records=4242)

    assert fin.database_records(str(index)) == 4242


def test_index_facts_read_what_the_builder_wrote(tmp_path):
    index = tmp_path / "index"
    build_fake_index(index, records=7, desc_length=12, page=4096)

    facts = fin.index_facts(str(index))

    assert facts["format_version"] == 1
    assert facts["descriptor_length"] == 12
    assert facts["record_page_length"] == 4096
    assert facts["record_count"] == 7


def test_finalize_refuses_a_directory_that_is_not_an_index(tmp_path):
    config = make_config(tmp_path, tmp_path / "nowhere")

    with pytest.raises(FileNotFoundError, match="does not look like a built index"):
        fin.finalize(config)


def test_finalize_records_the_transform_digest(tmp_path):
    index = tmp_path / "index"
    build_fake_index(index, records=120)
    config = make_config(tmp_path, index)
    prepared_corpus(config["output"])

    resolved = fin.finalize(config)

    assert resolved["record_count"] == 120
    assert resolved["projection"]["dim"] == 10
    assert len(resolved["projection"]["digest"]) == 64
    assert resolved["corpus"]["source_records"] == 120
    # No fingerprint file was written, so none is claimed.
    assert resolved["fingerprint"] is None

    on_disk = yaml.safe_load((index / "config.resolved.yaml").read_text())
    assert on_disk == resolved

    transform = (index / "transform.json").read_text()
    assert resolved["projection"]["digest"] in transform


def test_a_different_basis_gives_a_different_digest(tmp_path):
    mean = np.zeros(4)
    first = np.eye(4)
    second = np.roll(np.eye(4), 1, axis=1)

    assert cfg.transform_digest(mean, first) != cfg.transform_digest(mean, second)


def test_projection_none_ships_no_transform(tmp_path):
    index = tmp_path / "index"
    build_fake_index(index, records=120, desc_length=16)

    data = {
        "output": str(tmp_path / "prepared"),
        "corpus": {"input": str(tmp_path / "corpus")},
        "projection": {"method": "none"},
        "index": {"directory": str(index)},
    }
    path = tmp_path / "none.yaml"
    path.write_text(yaml.safe_dump(data))
    config = cfg.load(str(path))

    embedding = os.path.join(config["output"], "embedding")
    os.makedirs(embedding, exist_ok=True)

    resolved = fin.finalize(config)

    assert resolved["projection"]["digest"] is None
    assert not (index / "transform.json").exists()
