"""Service config: libraries are declared here, never in code."""

import os

import yaml


class ConfigError(Exception):
    """A service config that cannot be used as written."""


DEFAULTS = {
    "host": "127.0.0.1",
    "port": 8000,
    "embedding": {"model": "salsa16", "revision": None, "device": None},
    "cache": {"ttl_seconds": 21600, "max_entries": 5000},
    "limits": {"max_smiles_length": 400, "max_num_neighbors": 96},
    "static_dir": None,
}

LIBRARY_DEFAULTS = {
    "name": None,
    "display": None,
    "description": "",
    "url": None,
    "max_neighbors": 1024,
    "num_neighbors": 8,
}


def _merge(defaults, given, prefix=""):
    unknown = set(given) - set(defaults)
    if unknown:
        raise ConfigError(f"unknown {'key' if len(unknown) == 1 else 'keys'} "
                          f"{', '.join(sorted(prefix + k for k in unknown))}")

    out = {}
    for key, default in defaults.items():
        value = given.get(key, default)
        out[key] = _merge(default, value or {}, f"{prefix}{key}.") \
            if isinstance(default, dict) else value

    return out


def load(path):
    """Validated service config with defaults filled in."""

    with open(path) as handle:
        raw = yaml.safe_load(handle) or {}

    libraries = raw.pop("libraries", None)
    if not libraries:
        raise ConfigError(f"{path}: at least one library is required")

    config = _merge(DEFAULTS, raw)
    config["libraries"] = [_library(entry, i, path) for i, entry in enumerate(libraries)]

    names = [library["name"] for library in config["libraries"]]
    if len(set(names)) != len(names):
        raise ConfigError(f"{path}: duplicate library names")

    if config["static_dir"] and not os.path.isdir(config["static_dir"]):
        raise ConfigError(f"{path}: static_dir {config['static_dir']} is not a directory")

    return config


def _library(entry, position, path):
    merged = _merge(LIBRARY_DEFAULTS, entry, f"libraries[{position}].")

    for required in ("name", "url"):
        if not merged[required]:
            raise ConfigError(f"{path}: libraries[{position}].{required} is required")

    if merged["num_neighbors"] > merged["max_neighbors"]:
        raise ConfigError(
            f"{path}: libraries[{position}] serves more neighbors than it retrieves")

    merged["display"] = merged["display"] or merged["name"]

    return merged
