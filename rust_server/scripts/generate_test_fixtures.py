"""Regenerate the random-descriptor fixtures the kd_tree tests read."""

import argparse
import os
import random

# Must exceed the k=50 used by the tests
DEFAULT_SPECS = [
    # (filename, number of records)
    ("random_descriptors.txt", 10_000),
    ("small_random_descriptors.txt", 2_000),
]

# `TreeConfig::default()` uses desc_length 8
DIM = 8

SEED = 20240101


def write_fixture(path, n_records, dim, rng):
    header = "identifier," + ",".join(f"dim_{i}" for i in range(dim))

    with open(path, "w") as handle:
        handle.write(header + "\n")

        for i in range(n_records):
            # `CompoundIdentifier::from_string` asserts ASCII and length <= 50.
            identifier = f"TESTCMPD{i:08d}"

            # Uniform over [-1, 1), matching `Descriptor::random`.
            values = ",".join(f"{rng.uniform(-1.0, 1.0):.7f}" for _ in range(dim))

            handle.write(f"{identifier},{values}\n")

    return n_records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "kd_tree", "test_data"),
        help="directory to write fixtures into (default: kd_tree/test_data)",
    )
    parser.add_argument("--dim", type=int, default=DIM, help=f"descriptor length (default: {DIM})")
    parser.add_argument("--seed", type=int, default=SEED, help=f"RNG seed (default: {SEED})")
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    rng = random.Random(args.seed)

    for filename, n_records in DEFAULT_SPECS:
        path = os.path.join(out_dir, filename)
        written = write_fixture(path, n_records, args.dim, rng)
        print(f"wrote {written} records x {args.dim} dims -> {path}")


if __name__ == "__main__":
    main()
