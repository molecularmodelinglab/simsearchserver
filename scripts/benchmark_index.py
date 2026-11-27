#!/usr/bin/env python3
"""Measure recall and query latency for a built kd-tree index.

Serves the index, draws query vectors from the source embeddings, and compares
the server's hits against an exhaustive scan of the same embeddings. Recall is
exact, not estimated: brute force runs over every row the index was built from.

    python3 rust_server/scripts/benchmark_index.py \
        --index data/index/enamine_salsa16 \
        --source "data/enamine_in_stock_salsa16/*.parquet" \
        --desc-length 16

Add --json <path> to append a result row, so several dimensions can be
compared from one file.
"""

import argparse
import glob
import json
import os
import statistics
import subprocess
import time
import socket
import urllib.error
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER = os.path.join(REPO, "rust_server", "target", "release", "server")


def load_embeddings(pattern, desc_length):
    import numpy as np
    import pyarrow.parquet as pq

    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"no files matched {pattern!r}")

    dims = [f"dim_{i}" for i in range(desc_length)]
    blocks, ids = [], []

    for path in files:
        table = pq.read_table(path, columns=["ID"] + dims)
        blocks.append(np.stack([table[d].to_numpy() for d in dims], axis=1).astype(np.float32))
        ids.extend(table["ID"].to_pylist())

    return np.concatenate(blocks, axis=0), np.array(ids)


def wait_for_server(port, proc, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited during startup")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as r:
                return json.load(r)
        except (urllib.error.URLError, OSError):
            time.sleep(0.5)
    raise TimeoutError("server did not come up")


def query(port, k, vector):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/nn/{k}",
        data=json.dumps([float(x) for x in vector]).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def brute_force_ids(X, ids, vector, k, chunk=500_000):
    """Exact top-k by Euclidean distance, chunked to bound peak memory."""
    import numpy as np

    best_d = np.empty(0, dtype=np.float32)
    best_i = np.empty(0, dtype=np.int64)

    for start in range(0, len(X), chunk):
        block = X[start:start + chunk]
        d = np.linalg.norm(block - vector, axis=1)
        take = min(k, len(d))
        idx = np.argpartition(d, take - 1)[:take]
        best_d = np.concatenate([best_d, d[idx]])
        best_i = np.concatenate([best_i, idx + start])

    order = np.argsort(best_d)[:k]
    return set(ids[best_i[order]]), best_d[order]


def strip_prefix(identifier, prefix):
    return identifier[len(prefix):] if prefix and identifier.startswith(prefix) else identifier


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", required=True)
    parser.add_argument("--source", required=True, help="glob for the parquet files the index was built from")
    parser.add_argument("--desc-length", type=int, required=True)
    parser.add_argument("--k", type=int, nargs="+", default=[1, 10, 100])
    parser.add_argument("--queries", type=int, default=200, help="queries for the latency measurement")
    parser.add_argument("--recall-queries", type=int, default=50, help="queries for the recall measurement")
    parser.add_argument("--port", type=int, default=None,
                        help="default: an OS-assigned free port, so concurrent runs cannot collide")
    parser.add_argument("--id-prefix", default="enamine", help="prefix the builder prepended to source IDs")
    parser.add_argument("--label", default=None)
    parser.add_argument("--allow-size-mismatch", action="store_true",
                        help="permit --source to cover a different record count than the index "
                             "(e.g. an index that also holds molecules from another set)")
    parser.add_argument("--json", default=None, help="append results to this JSON file")
    args = parser.parse_args()

    import numpy as np

    label = args.label or os.path.basename(args.index.rstrip("/"))

    # A guessed port collides and the client talks to another run's server.
    if args.port is None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            args.port = probe.getsockname()[1]

    print(f"loading source embeddings from {args.source} ...")
    X, ids = load_embeddings(args.source, args.desc_length)
    print(f"  {X.shape[0]:,} x {X.shape[1]} float32 ({X.nbytes / 1e6:.0f} MB)")

    rng = np.random.default_rng(0)
    q_latency = X[rng.integers(0, len(X), size=args.queries)]
    q_recall = X[rng.integers(0, len(X), size=args.recall_queries)]

    proc = subprocess.Popen([SERVER, "--dirname", args.index, "--port", str(args.port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    result = {"label": label, "index": args.index, "desc_length": args.desc_length}

    try:
        t0 = time.time()
        health = wait_for_server(args.port, proc)
        result["startup_s"] = time.time() - t0
        result["num_records"] = health["num_records"]
        result["internal_nodes"] = health["num_internal_nodes"]
        print(f"  index: {health['num_records']:,} records, "
              f"{health['num_internal_nodes']:,} internal nodes, "
              f"loaded in {result['startup_s']:.1f}s")

        # A --source that does not match the index measures recall against the wrong set.
        if health["num_records"] != X.shape[0] and not args.allow_size_mismatch:
            raise SystemExit(
                f"index holds {health['num_records']:,} records but --source covers "
                f"{X.shape[0]:,}. Recall would be measured against the wrong set. "
                f"Narrow --source to the files the index was built from, or pass "
                f"--allow-size-mismatch if the difference is intended.")

        if health["desc_length"] != args.desc_length:
            raise SystemExit(
                f"index desc_length {health['desc_length']} != --desc-length {args.desc_length}; "
                f"port {args.port} may be serving a different index")

        print("\nlatency")
        result["latency"] = {}
        for k in args.k:
            query(args.port, k, q_latency[0])
            times = []
            for v in q_latency:
                t0 = time.perf_counter()
                query(args.port, k, v)
                times.append((time.perf_counter() - t0) * 1000)
            times.sort()
            med = statistics.median(times)
            p95 = times[int(0.95 * (len(times) - 1))]
            result["latency"][str(k)] = {"median_ms": med, "p95_ms": p95, "min_ms": times[0]}
            print(f"  k={k:<4} n={len(times)}  median={med:7.2f} ms  p95={p95:7.2f} ms  min={times[0]:7.2f} ms")

        print("\nrecall vs exhaustive scan")
        result["recall"] = {}
        for k in args.k:
            hits_found = 0
            exact_sets = 0
            for v in q_recall:
                truth, _ = brute_force_ids(X, ids, v, k)
                got = {strip_prefix(h["identifier"], args.id_prefix) for h in query(args.port, k, v)["hits"]}
                hits_found += len(truth & got)
                exact_sets += (truth == got)
            recall = hits_found / (k * len(q_recall))
            result["recall"][str(k)] = {"recall": recall, "exact_sets": exact_sets, "queries": len(q_recall)}
            print(f"  k={k:<4} recall={recall:7.4f}   exact top-k sets: {exact_sets}/{len(q_recall)}")

    finally:
        proc.terminate()
        proc.wait(timeout=60)

    if args.json:
        rows = []
        if os.path.exists(args.json):
            with open(args.json) as f:
                rows = json.load(f)
        rows = [r for r in rows if r.get("label") != label] + [result]
        with open(args.json, "w") as f:
            json.dump(rows, f, indent=2)
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()
