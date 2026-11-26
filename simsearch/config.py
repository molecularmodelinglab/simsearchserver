"""Preparation config: one file is the source of truth for every stage."""

import hashlib
import os

import yaml

# Each entry is (default, type). A key absent from here is rejected, so a typo
# fails loudly instead of silently taking the default.
SCHEMA = {
    "corpus": {
        "input": (None, str),
        "id_column": ("ID", str),
        "smiles_column": ("SMILES", str),
        "id_prefix": ("", str),
    },
    "embedding": {
        "model": ("salsa16", str),
        "revision": (None, (str, type(None))),
        "batch_size": (1024, int),
        "device": (None, (str, type(None))),
    },
    "projection": {
        "method": ("pca", str),
        "dim": (10, int),
        "fit_rows": (4_000_000, int),
        "seed": (0, int),
    },
    "fingerprint": {
        "type": ("atompair", str),
        "bits": (512, int),
        "count_simulation": (False, bool),
        "metric": ("tanimoto", str),
    },
    "index": {
        "directory": (None, str),
        "record_page_length": (16384, int),
    },
    "serving": {
        "max_neighbors": (1024, int),
        "num_neighbors": (8, int),
        "warm": (True, bool),
    },
    "output": (None, str),
}

PROJECTION_METHODS = ("pca", "none")
FINGERPRINT_TYPES = ("atompair", "morgan")
METRICS = ("tanimoto", "dice")


class ConfigError(Exception):
    """A config that cannot be used as written."""


def load(path):
    """Validated config from a YAML file, with defaults filled in."""

    with open(path) as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        raise ConfigError(f"{path} must contain a mapping")

    config = _apply(SCHEMA, raw, path, prefix="")
    _check(config, path)

    return config


def _apply(schema, raw, path, prefix):
    unknown = set(raw) - set(schema)
    if unknown:
        raise ConfigError(
            f"{path}: unknown {'key' if len(unknown) == 1 else 'keys'} "
            f"{', '.join(sorted(prefix + k for k in unknown))}")

    out = {}
    for key, spec in schema.items():
        if isinstance(spec, dict):
            out[key] = _apply(spec, raw.get(key) or {}, path, f"{prefix}{key}.")
            continue

        default, expected = spec
        value = raw.get(key, default)

        if value is None and default is None and expected is str:
            raise ConfigError(f"{path}: {prefix}{key} is required")

        if value is not None and not isinstance(value, expected):
            raise ConfigError(
                f"{path}: {prefix}{key} must be {getattr(expected, '__name__', expected)}")

        out[key] = value

    return out


def _check(config, path):
    if config["projection"]["method"] not in PROJECTION_METHODS:
        raise ConfigError(
            f"{path}: projection.method must be one of {', '.join(PROJECTION_METHODS)}")

    if config["fingerprint"]["type"] not in FINGERPRINT_TYPES:
        raise ConfigError(
            f"{path}: fingerprint.type must be one of {', '.join(FINGERPRINT_TYPES)}")

    if config["fingerprint"]["metric"] not in METRICS:
        raise ConfigError(f"{path}: fingerprint.metric must be one of {', '.join(METRICS)}")

    if config["fingerprint"]["bits"] % 8:
        raise ConfigError(f"{path}: fingerprint.bits must be a multiple of 8")

    if config["serving"]["num_neighbors"] > config["serving"]["max_neighbors"]:
        raise ConfigError(f"{path}: serving.num_neighbors exceeds serving.max_neighbors")


def stage_directory(config, stage):
    """Where a stage writes its shards."""
    return os.path.join(config["output"], stage)


def descriptor_length(config):
    """Dimension the index is built at, after any projection."""

    if config["projection"]["method"] == "none":
        return None

    return config["projection"]["dim"]


def transform_digest(mean, components):
    """Identifies a projection; dimension alone does not."""

    digest = hashlib.sha256()
    for array in (mean, components):
        digest.update(array.astype("<f8").tobytes())

    return digest.hexdigest()
