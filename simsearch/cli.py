"""Command line entry point for corpus preparation."""

import argparse
import glob
import os
import sys

from simsearch import config as cfg
from simsearch import finalize as finalize_stage
from simsearch import stages

STAGES = ("embed", "project", "fingerprint")


def _builder_command(config):
    source = stages.index_source_directory(config)
    prefix = config["corpus"]["id_prefix"]

    lines = [
        "rust_server/target/release/builder build-from-files",
        f"    --filenames {source}/*.parquet",
        f"    --config-filename {os.path.join(config['output'], 'index.yaml')}",
        f"    --fingerprints {cfg.stage_directory(config, 'fingerprint')}",
    ]

    if prefix:
        lines.append(f"    --id-prefix {prefix}")

    return " \\\n".join(lines)


def _write_index_config(config):
    path = os.path.join(config["output"], "index.yaml")
    length = cfg.descriptor_length(config)

    if length is None:
        sources = sorted(glob.glob(os.path.join(stages.index_source_directory(config),
                                                "*.parquet")))
        if not sources:
            return None
        import pyarrow.parquet as pq
        names = pq.ParquetFile(sources[0]).schema_arrow.names
        length = len([n for n in names if n.startswith("dim_")])

    os.makedirs(config["output"], exist_ok=True)
    with open(path, "w") as handle:
        handle.write(f'directory: "{config["index"]["directory"]}"\n')
        handle.write(f"desc_length: {length}\n")
        handle.write(f"record_page_length: {config['index']['record_page_length']}\n")

    return path


def main(argv=None):
    """Runs a preparation stage, or all of them."""

    parser = argparse.ArgumentParser(
        prog="simsearch",
        description="Prepare a molecule library for indexing.")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("prepare",) + STAGES + ("finalize",):
        helps = {
            "prepare": "run every stage in order",
            "finalize": "record what the built index was made from",
        }
        stage = sub.add_parser(name, help=helps.get(name, f"run the {name} stage"))
        stage.add_argument("config")
        stage.add_argument("--shard", type=int, default=0,
                           help="this task's index, for a slurm array")
        stage.add_argument("--of", type=int, default=1, help="number of tasks")
        if name in ("prepare", "fingerprint"):
            stage.add_argument("--processes", type=int, default=0)

    args = parser.parse_args(argv)

    try:
        config = cfg.load(args.config)
    except (cfg.ConfigError, FileNotFoundError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    if args.command == "finalize":
        try:
            finalize_stage.finalize(config)
        except (FileNotFoundError, KeyError) as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        return 0

    requested = STAGES if args.command == "prepare" else (args.command,)

    for name in requested:
        if name == "embed":
            stages.embed(config, args.shard, args.of)
        elif name == "project":
            stages.project(config, args.shard, args.of)
        else:
            stages.fingerprint(config, args.shard, args.of,
                               getattr(args, "processes", 0))

    if args.command == "prepare":
        written = _write_index_config(config)
        if written:
            print(f"\nwrote {written}; build the index with:\n\n{_builder_command(config)}")
            print("\nthen record what it was built from:\n\n"
                  f"    simsearch finalize {args.config}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
