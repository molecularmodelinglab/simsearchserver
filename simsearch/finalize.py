"""Records what an index was built from, so a mismatch is caught not served."""

import glob
import json
import os
import struct

import numpy as np
import pyarrow.parquet as pq
import yaml

from simsearch import config as cfg
from simsearch import stages

RESOLVED_FILENAME = "config.resolved.yaml"
TRANSFORM_FILENAME = "transform.json"

# magic, version, padding, then the entry count.
_DB_COUNT_AT = 16


def database_records(directory):
    """Record count as the index itself reports it."""

    path = os.path.join(directory, "db.db")
    with open(path, "rb") as handle:
        header = handle.read(_DB_COUNT_AT + 8)

    return struct.unpack_from("<Q", header, _DB_COUNT_AT)[0]


def index_facts(directory):
    """What the builder wrote about the index."""

    with open(os.path.join(directory, "config.yaml")) as handle:
        built = yaml.safe_load(handle)

    return {
        "format_version": built.get("format_version"),
        "descriptor_length": built.get("desc_length"),
        "record_page_length": built.get("record_page_length"),
        "record_count": database_records(directory),
        "built_at": built.get("built_at"),
    }


def _fingerprint_facts(config, directory):
    manifests = sorted(glob.glob(os.path.join(
        cfg.stage_directory(config, "fingerprint"), "*.json")))

    if not os.path.exists(os.path.join(directory, "fingerprints")):
        return None

    settings = dict(config["fingerprint"])

    if manifests:
        with open(manifests[0]) as handle:
            settings["bytes_per_record"] = json.load(handle)["bytes_per_record"]

    return settings


def _write_transform(config, directory):
    """Ships the projection inside the index, so there is one copy of it."""

    if config["projection"]["method"] == "none":
        return None

    model_path = os.path.join(cfg.stage_directory(config, "projection"), stages.PCA_FILENAME)
    loaded = np.load(model_path)
    mean, components = loaded["mean"], loaded["components"]
    dim = config["projection"]["dim"]

    digest = cfg.transform_digest(mean, components[:, :dim])

    payload = {
        "method": "pca",
        "source_dim": int(mean.shape[0]),
        "dim": dim,
        "digest": digest,
        "mean": [float(x) for x in mean],
        "components": [[float(x) for x in row] for row in components[:, :dim]],
    }

    with open(os.path.join(directory, TRANSFORM_FILENAME), "w") as handle:
        json.dump(payload, handle)

    return digest


def finalize(config):
    """Writes the resolved manifest and the transform into the index."""

    directory = config["index"]["directory"]

    if not os.path.exists(os.path.join(directory, "config.yaml")):
        raise FileNotFoundError(f"{directory} does not look like a built index")

    facts = index_facts(directory)
    digest = _write_transform(config, directory)

    sources = sorted(glob.glob(os.path.join(stages.index_source_directory(config), "*.parquet")))
    source_records = sum(pq.ParquetFile(p).metadata.num_rows for p in sources)

    resolved = {
        "built_at": facts["built_at"],
        "format_version": facts["format_version"],
        "descriptor_length": facts["descriptor_length"],
        "record_page_length": facts["record_page_length"],
        "record_count": facts["record_count"],
        "embedding": {
            "model": config["embedding"]["model"],
            "revision": config["embedding"]["revision"],
        },
        "projection": {
            "method": config["projection"]["method"],
            "dim": cfg.descriptor_length(config),
            # Dimension alone does not identify a transform.
            "digest": digest,
        },
        "fingerprint": _fingerprint_facts(config, directory),
        "corpus": {
            "id_prefix": config["corpus"]["id_prefix"],
            "source_records": source_records,
        },
        "serving": dict(config["serving"]),
    }

    path = os.path.join(directory, RESOLVED_FILENAME)
    with open(path, "w") as handle:
        yaml.safe_dump(resolved, handle, sort_keys=True)

    if facts["record_count"] != source_records:
        print(f"note: {facts['record_count']:,} records indexed from "
              f"{source_records:,} source rows; the difference was skipped at build")

    print(f"wrote {path}")

    return resolved
