"""HTTP API: SMILES in, ranked analogs out."""

import asyncio
import csv
import io
import logging
import os
import sys
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from simsearch_api import batch as batching
from simsearch_api import config as cfg
from simsearch_api.cache import ResultCache
from simsearch_api.chemistry import Chemistry, InvalidSmiles, ModelUnavailable
from simsearch_api.libraries import Library, LibraryError

CONFIG_ENV = "SIMSEARCH_API_CONFIG"

CSV_COLUMNS = ["row", "query", "status", "canonical", "note", "error",
               "rank", "identifier", "smiles", "distance", "similarity"]

log = logging.getLogger("simsearch")


class SearchBody(BaseModel):
    smiles: str
    library: str | None = None
    num_neighbors: int | None = Field(default=None, ge=1)


class BatchBody(BaseModel):
    smiles: list[str]
    library: str | None = None
    num_neighbors: int | None = Field(default=None, ge=1)
    format: str = Field(default="json", pattern="^(json|csv)$")


class State:
    """Everything a request is answered from."""

    def __init__(self):
        self.config = None
        self.chemistry = None
        self.libraries = {}
        self.cache = None
        self.client = None
        self.batch_gate = None


state = State()


@asynccontextmanager
async def lifespan(_app):
    path = os.environ.get(CONFIG_ENV)

    if not path:
        raise RuntimeError(f"set {CONFIG_ENV} to a service config")

    state.config = cfg.load(path)
    state.cache = ResultCache(**state.config["cache"])
    state.client = httpx.AsyncClient()
    state.batch_gate = asyncio.Semaphore(state.config["limits"]["batch_concurrency"])

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
        prepared = state.chemistry.prepare(smiles)
    except InvalidSmiles as error:
        raise HTTPException(400, str(error))

    prepared_at = time.perf_counter()

    # Inference holds the GIL, so it runs off the event loop.
    embedding = await asyncio.to_thread(state.chemistry.embed, prepared.key)
    embedded_at = time.perf_counter()

    descriptor = library.project(embedding)
    projected_at = time.perf_counter()

    try:
        result = await library.search(
            state.client, descriptor, prepared.fingerprint, wanted)
    except LibraryError as error:
        raise HTTPException(502, str(error))

    finished = time.perf_counter()

    query = {
        "smiles": smiles,
        "canonical": prepared.canonical,
        "library": library.name,
    }

    if prepared.note:
        query["note"] = prepared.note

    answer = {
        "query": query,
        "hits": result["hits"],
        "timing": {
            "prepare_ms": (prepared_at - started) * 1000,
            "embed_ms": (embedded_at - prepared_at) * 1000,
            "project_ms": (projected_at - embedded_at) * 1000,
            "search_ms": (finished - projected_at) * 1000,
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


async def _search_batch(body):
    limits = state.config["limits"]
    submitted = body.smiles

    if not submitted:
        raise HTTPException(400, "no SMILES supplied")

    if len(submitted) > limits["max_batch_size"]:
        raise HTTPException(400, f"{len(submitted)} structures exceeds the batch "
                                 f"limit of {limits['max_batch_size']}")

    library = _library_for(body.library)
    wanted = body.num_neighbors or library.num_neighbors

    if wanted > limits["max_num_neighbors"]:
        raise HTTPException(400, f"num_neighbors {wanted} exceeds "
                                 f"{limits['max_num_neighbors']}")

    if state.batch_gate.locked():
        raise HTTPException(429, "another batch is running; retry shortly")

    async with state.batch_gate:
        started = time.perf_counter()

        # Parsing 10,000 structures would stall the event loop.
        prepared = await asyncio.to_thread(
            batching.prepare, submitted, state.chemistry)
        prepared_at = time.perf_counter()

        embeddings = await asyncio.to_thread(
            state.chemistry.embed_many, [item.key for item in prepared.unique])
        embedded_at = time.perf_counter()

        hits = []

        if prepared.unique:
            descriptors = library.project(embeddings)

            try:
                answer = await library.search_batch(
                    state.client, descriptors,
                    [item.fingerprint for item in prepared.unique],
                    wanted, limits["batch_timeout_seconds"])
            except LibraryError as error:
                raise HTTPException(502, str(error))

            hits = [result["hits"] for result in answer["results"]]

        finished = time.perf_counter()

    results = []

    for row in prepared.rows:
        if not row.ok:
            results.append({"query": row.smiles, "status": "error",
                            "error": row.error})
            continue

        entry = {
            "query": row.smiles,
            "status": "ok",
            "canonical": row.prepared.canonical,
            "hits": hits[prepared.assignment[row.index]],
        }

        if row.prepared.note:
            entry["note"] = row.prepared.note

        results.append(entry)

    log.info("batch %s: %d submitted, %d unique, %d failed in %.0f ms",
             library.name, len(submitted), len(prepared.unique),
             prepared.counts["failed"], (finished - started) * 1000)

    return {
        "library": library.name,
        "num_neighbors": wanted,
        "counts": prepared.counts,
        "timing": {
            "prepare_ms": (prepared_at - started) * 1000,
            "embed_ms": (embedded_at - prepared_at) * 1000,
            "search_ms": (finished - embedded_at) * 1000,
            "total_ms": (finished - started) * 1000,
        },
        "results": results,
    }


def _as_csv(answer):
    """The same results as one row per hit, for a dataframe."""

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()

    for position, result in enumerate(answer["results"]):
        common = {
            "row": position,
            "query": result["query"],
            "status": result["status"],
            "canonical": result.get("canonical", ""),
            "note": result.get("note", ""),
            "error": result.get("error", ""),
        }

        if result["status"] != "ok" or not result["hits"]:
            writer.writerow(common)
            continue

        for rank, hit in enumerate(result["hits"], start=1):
            writer.writerow({
                **common,
                "rank": rank,
                "identifier": hit["identifier"],
                "smiles": hit["smiles"],
                "distance": hit["distance"],
                "similarity": hit.get("similarity", ""),
            })

    return buffer.getvalue()


@app.post("/api/search/batch")
async def search_batch(body: BatchBody):
    """Ranked analogs for many structures, embedded in one pass."""

    answer = await _search_batch(body)

    if body.format == "csv":
        return Response(
            content=_as_csv(answer),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="simsearch.csv"'})

    return answer


@app.get("/api/libraries")
async def libraries():
    """What can be searched, for the selector and its description."""
    return {"libraries": [library.describe() for library in state.libraries.values()],
            "default": next(iter(state.libraries), None),
            "limits": {
                "max_num_neighbors": state.config["limits"]["max_num_neighbors"],
                "max_smiles_length": min(state.config["limits"]["max_smiles_length"],
                                         state.chemistry.max_smiles_length),
                "max_batch_size": state.config["limits"]["max_batch_size"],
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
