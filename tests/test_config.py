"""Tests for preparation config loading and validation."""

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simsearch import config as cfg

MINIMAL = {
    "output": "/tmp/prepared",
    "corpus": {"input": "/tmp/corpus"},
    "index": {"directory": "/tmp/index"},
}


def write(tmp_path, data):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    return str(path)


def test_defaults_are_filled_in(tmp_path):
    config = cfg.load(write(tmp_path, MINIMAL))

    assert config["corpus"]["id_column"] == "ID"
    assert config["embedding"]["model"] == "salsa16"
    assert config["projection"]["dim"] == 10
    assert config["fingerprint"]["bits"] == 512
    assert config["fingerprint"]["count_simulation"] is False
    assert config["index"]["record_page_length"] == 16384


def test_a_missing_required_key_is_rejected(tmp_path):
    data = {"output": "/tmp/prepared", "index": {"directory": "/tmp/index"}}

    with pytest.raises(cfg.ConfigError, match="corpus.input is required"):
        cfg.load(write(tmp_path, data))


def test_a_misspelled_key_is_rejected(tmp_path):
    data = dict(MINIMAL, projection={"methd": "pca"})

    with pytest.raises(cfg.ConfigError, match="unknown key projection.methd"):
        cfg.load(write(tmp_path, data))


def test_an_unknown_top_level_key_is_rejected(tmp_path):
    data = dict(MINIMAL, fingerprints={"bits": 512})

    with pytest.raises(cfg.ConfigError, match="unknown key fingerprints"):
        cfg.load(write(tmp_path, data))


def test_an_unsupported_projection_method_is_rejected(tmp_path):
    data = dict(MINIMAL, projection={"method": "lda"})

    with pytest.raises(cfg.ConfigError, match="projection.method"):
        cfg.load(write(tmp_path, data))


def test_projection_none_is_supported(tmp_path):
    data = dict(MINIMAL, projection={"method": "none"})
    config = cfg.load(write(tmp_path, data))

    assert cfg.descriptor_length(config) is None


def test_bits_must_be_a_whole_number_of_bytes(tmp_path):
    data = dict(MINIMAL, fingerprint={"bits": 500})

    with pytest.raises(cfg.ConfigError, match="multiple of 8"):
        cfg.load(write(tmp_path, data))


def test_serving_cannot_display_more_than_it_retrieves(tmp_path):
    data = dict(MINIMAL, serving={"max_neighbors": 8, "num_neighbors": 128})

    with pytest.raises(cfg.ConfigError, match="exceeds"):
        cfg.load(write(tmp_path, data))


def test_a_wrongly_typed_value_is_rejected(tmp_path):
    data = dict(MINIMAL, projection={"dim": "ten"})

    with pytest.raises(cfg.ConfigError, match="projection.dim must be int"):
        cfg.load(write(tmp_path, data))


def test_ids_are_not_prefixed_by_default(tmp_path):
    config = cfg.load(write(tmp_path, MINIMAL))

    assert config["corpus"]["id_prefix"] == ""


def test_an_id_prefix_reaches_the_builder_command(tmp_path):
    from simsearch.cli import _builder_command

    plain = cfg.load(write(tmp_path, MINIMAL))
    assert "--id-prefix" not in _builder_command(plain)

    prefixed = cfg.load(write(tmp_path, dict(
        MINIMAL, corpus=dict(MINIMAL["corpus"], id_prefix="real"))))
    assert "--id-prefix real" in _builder_command(prefixed)


def test_stage_directories_come_from_one_output_root(tmp_path):
    config = cfg.load(write(tmp_path, MINIMAL))

    assert cfg.stage_directory(config, "embedding") == "/tmp/prepared/embedding"
    assert cfg.stage_directory(config, "fingerprint") == "/tmp/prepared/fingerprint"


def test_transform_digest_distinguishes_bases():
    import numpy as np

    mean = np.zeros(4)
    first = np.eye(4)
    second = np.eye(4)[:, ::-1].copy()

    assert cfg.transform_digest(mean, first) == cfg.transform_digest(mean, first)
    assert cfg.transform_digest(mean, first) != cfg.transform_digest(mean, second)
    assert cfg.transform_digest(mean + 1, first) != cfg.transform_digest(mean, first)
