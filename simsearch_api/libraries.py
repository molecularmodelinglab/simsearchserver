"""One kd-tree backend per library, each with its own projection."""

import numpy as np

TRANSFORM_TIMEOUT = 30.0


class LibraryError(Exception):
    """A library that cannot be served."""


class Library:
    """A kd-tree backend and the projection its index was built with."""

    def __init__(self, settings):
        self.name = settings["name"]
        self.display = settings["display"]
        self.description = settings["description"]
        self.url = settings["url"].rstrip("/")
        self.max_neighbors = settings["max_neighbors"]
        self.num_neighbors = settings["num_neighbors"]

        self.desc_length = None
        self.num_records = None
        self.fingerprint_bytes = None
        self.digest = None
        self._mean = None
        self._components = None

    async def connect(self, client):
        """Reads the index's shape and projection, refusing a mismatch."""

        health = await self._get(client, "/health")

        self.desc_length = health["desc_length"]
        self.num_records = health["num_records"]
        self.fingerprint_bytes = health.get("fingerprint_bytes")

        transform = await self._get(client, "/transform")

        if transform["dim"] != self.desc_length:
            raise LibraryError(
                f"{self.name}: /transform projects to {transform['dim']} dimensions "
                f"but the index was built for {self.desc_length}")

        self.digest = transform["digest"]
        self._mean = np.asarray(transform["mean"], dtype=np.float32)
        self._components = np.asarray(transform["components"], dtype=np.float32)

        if self._components.shape != (self._mean.shape[0], self.desc_length):
            raise LibraryError(f"{self.name}: /transform components are the wrong shape")

    async def _get(self, client, path):
        try:
            response = await client.get(f"{self.url}{path}", timeout=TRANSFORM_TIMEOUT)
        except Exception as error:
            raise LibraryError(f"{self.name}: {self.url}{path} unreachable: {error}")

        if response.status_code != 200:
            raise LibraryError(
                f"{self.name}: {self.url}{path} returned {response.status_code}. "
                "An index built without a projection cannot be served, because the "
                "query would not be projected to its dimension.")

        return response.json()

    def project(self, embedding):
        """The query descriptor in this index's space."""
        return (np.asarray(embedding, dtype=np.float32) - self._mean) @ self._components

    @property
    def source_dim(self):
        return None if self._mean is None else int(self._mean.shape[0])

    async def search(self, client, descriptor, fingerprint, num_neighbors):
        """Ranked hits from this library's backend."""

        payload = {
            "descriptor": [float(x) for x in descriptor],
            "num_neighbors": num_neighbors,
        }

        if fingerprint is not None and self.fingerprint_bytes:
            payload["fingerprint"] = [int(b) for b in fingerprint]

        response = await client.post(
            f"{self.url}/nn/{self.max_neighbors}", json=payload, timeout=TRANSFORM_TIMEOUT)

        if response.status_code != 200:
            raise LibraryError(f"{self.name}: search returned {response.status_code}: "
                               f"{response.text[:200]}")

        return response.json()

    def describe(self):
        return {
            "name": self.name,
            "display": self.display,
            "description": self.description,
            "num_records": self.num_records,
            "desc_length": self.desc_length,
            "has_fingerprints": bool(self.fingerprint_bytes),
            "transform_digest": self.digest,
        }
