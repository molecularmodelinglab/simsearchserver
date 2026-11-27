"""HTTP API: SMILES in, ranked analogs out."""

import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from simsearch_api import config as cfg
from simsearch_api.cache import ResultCache
from simsearch_api.chemistry import Chemistry, InvalidSmiles, ModelUnavailable
from simsearch_api.libraries import Library, LibraryError

CONFIG_ENV = "SIMSEARCH_API_CONFIG"

log = logging.getLogger("simsearch")


class SearchBody(BaseModel):
    smiles: str
    library: str | None = None
    num_neighbors: int | None = Field(default=None, ge=1)


class State:
    """Everything a request is answered from."""

    def __init__(self):
        self.config = None
        self.chemistry = None
        self.libraries = {}
        self.cache = None
        self.client = None


state = State()


@asynccontextmanager
async def lifespan(_app):
    path = os.environ.get(CONFIG_ENV)

    if not path:
        raise RuntimeError(f"set {CONFIG_ENV} to a service config")

    state.config = cfg.load(path)
    state.cache = ResultCache(**state.config["cache"])
    state.client = httpx.AsyncClient()

    try:
        state.chemistry = Chemistry(state.config["embedding"])
    except ModelUnavailable as error:
        print(f"\nERROR: {error}\n", file=sys.stderr, flush=True)
        raise
    log.info("loaded %s, embedding dimension %d",
             state.chemistry.model_name, state.chemistry.dim)

    for settings in state.config["libraries"]:
        library = Library(settings)
        await library.connect(state.client)

        if library.source_dim != state.chemistry.dim:
            raise LibraryError(
                f"{library.name}: projection expects {library.source_dim} dimensions "
                f"but {state.chemistry.model_name} produces {state.chemistry.dim}")

        state.libraries[library.name] = library
        log.info("library %s: %d records, dim %d, transform %s",
                 library.name, library.num_records, library.desc_length,
                 library.digest[:12])

    mount_static()

    yield

    await state.client.aclose()


app = FastAPI(title="SmallSA", lifespan=lifespan)


def _library_for(name):
    if name is None:
        return next(iter(state.libraries.values()))

    library = state.libraries.get(name)

    if library is None:
        raise HTTPException(404, f"unknown library {name!r}; "
                                 f"available: {', '.join(state.libraries)}")

    return library


async def _search(smiles, library_name, num_neighbors):
    limits = state.config["limits"]
    smiles = (smiles or "").strip()

    if not smiles:
        raise HTTPException(400, "no SMILES supplied")

    longest = min(limits["max_smiles_length"], state.chemistry.max_smiles_length)

    if len(smiles) > longest:
        raise HTTPException(400, f"SMILES is {len(smiles)} characters, "
                                 f"the limit is {longest}")

    library = _library_for(library_name)
    wanted = num_neighbors or library.num_neighbors

    if wanted > limits["max_num_neighbors"]:
        raise HTTPException(400, f"num_neighbors {wanted} exceeds "
                                 f"{limits['max_num_neighbors']}")

    key = (library.name, smiles, wanted)
    cached = state.cache.get(key)

    if cached is not None:
        return {**cached, "cached": True}

    started = time.perf_counter()

    try:
        molecule = state.chemistry.parse(smiles)
    except InvalidSmiles as error:
        raise HTTPException(400, str(error))

    # Inference holds the GIL, so it runs off the event loop.
    embedding = await asyncio.to_thread(state.chemistry.embed, smiles)
    embedded_at = time.perf_counter()

    descriptor = library.project(embedding)
    projected_at = time.perf_counter()

    fingerprint = state.chemistry.fingerprint(molecule)
    fingerprinted_at = time.perf_counter()

    try:
        result = await library.search(state.client, descriptor, fingerprint, wanted)
    except LibraryError as error:
        raise HTTPException(502, str(error))

    finished = time.perf_counter()

    answer = {
        "query": {
            "smiles": smiles,
            "canonical": state.chemistry.canonical(molecule),
            "library": library.name,
        },
        "hits": result["hits"],
        "timing": {
            "embed_ms": (embedded_at - started) * 1000,
            "project_ms": (projected_at - embedded_at) * 1000,
            "fingerprint_ms": (fingerprinted_at - projected_at) * 1000,
            "search_ms": (finished - fingerprinted_at) * 1000,
            "index_ms": result["timing"]["total"] / 1000,
            "total_ms": (finished - started) * 1000,
        },
        "cached": False,
    }

    state.cache.put(key, answer)

    log.info("%s %s -> %d hits in %.1f ms", library.name, smiles,
             len(answer["hits"]), answer["timing"]["total_ms"])

    return answer


@app.get("/api/search")
async def search_get(
    smiles: str = Query(..., description="query structure"),
    library: str | None = Query(default=None),
    n: int | None = Query(default=None, ge=1),
):
    """Ranked analogs of `smiles`. Linkable, so results can be shared."""
    return await _search(smiles, library, n)


@app.post("/api/search")
async def search_post(body: SearchBody):
    """Same as the GET form, for SMILES too long for a URL."""
    return await _search(body.smiles, body.library, body.num_neighbors)


@app.get("/api/libraries")
async def libraries():
    """What can be searched, for the selector and its description."""
    return {"libraries": [library.describe() for library in state.libraries.values()],
            "default": next(iter(state.libraries), None),
            "limits": {
                "max_num_neighbors": state.config["limits"]["max_num_neighbors"],
                "max_smiles_length": min(state.config["limits"]["max_smiles_length"],
                                         state.chemistry.max_smiles_length),
            }}


@app.get("/api/health")
async def health():
    """Whether the model and every library are answering."""

    reachable = {}
    for name, library in state.libraries.items():
        try:
            response = await state.client.get(f"{library.url}/health", timeout=5.0)
            reachable[name] = response.status_code == 200
        except Exception:
            reachable[name] = False

    everything = all(reachable.values())

    return JSONResponse(
        status_code=200 if everything else 503,
        content={
            "status": "ok" if everything else "degraded",
            "model": state.chemistry.model_name,
            "embedding_dim": state.chemistry.dim,
            "libraries": reachable,
            "cache": state.cache.stats(),
        })


def mount_static():
    """Serves the built frontend, after the API routes so they still win."""

    directory = state.config.get("static_dir") if state.config else None

    if not directory:
        log.info("no static_dir; serving the API only")
        return

    app.mount("/", StaticFiles(directory=directory, html=True), name="web")
    log.info("serving the frontend from %s", directory)
