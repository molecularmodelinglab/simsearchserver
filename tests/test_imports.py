"""Every module of every shipped package imports.

A file missing from a checkout — excluded by .gitignore, or never added — looks
fine until something imports it. CI checks out fresh, so this fails there.
"""

import importlib
import os
import pkgutil
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PACKAGES = ["simsearch", "simsearch_api"]


def modules_of(name):
    package = importlib.import_module(name)

    return [f"{name}.{found.name}"
            for found in pkgutil.iter_modules(package.__path__)]


@pytest.mark.parametrize("package", PACKAGES)
def test_every_module_imports(package):
    found = modules_of(package)

    assert found, f"{package} has no modules; is it missing from the checkout?"

    for module in found:
        importlib.import_module(module)
