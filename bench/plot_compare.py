#!/usr/bin/env python3
"""
Overlay latency CDFs from two benchmark run directories.
Usage:
  python3 bench/plot_compare.py RUN_DIR_A RUN_DIR_B OUT_PNG
Assumes each run dir has latencies.csv and summary.json.
"""
import sys
import json
import csv
from pathlib import Path

import matplotlib.pyplot as plt


def load_latencies(run_dir: Path):
    lat_file = run_dir / "latencies.csv"
    vals = []
    with open(lat_file) as f:
        rdr = csv.reader(f)
        next(rdr)  # header
        for _, v in rdr:
            vals.append(float(v))
    vals.sort()
    ys = [i / len(vals) for i in range(len(vals))]
    return vals, ys


def label(run_dir: Path):
    summary = json.load(open(run_dir / "summary.json"))
    gate = "Gate ON" if summary.get("conflict_gate", True) else "Gate OFF"
    n = summary.get("nodes")
    return f"{gate}, n={n}"


def main():
    if len(sys.argv) != 4:
        print("usage: plot_compare.py RUN_DIR_A RUN_DIR_B OUT_PNG")
        sys.exit(1)
    a = Path(sys.argv[1])
    b = Path(sys.argv[2])
    out = Path(sys.argv[3])

    lat_a, ys_a = load_latencies(a)
    lat_b, ys_b = load_latencies(b)

    plt.figure()
    plt.plot(lat_a, ys_a, label=label(a))
    plt.plot(lat_b, ys_b, label=label(b))
    plt.xlabel("latency (s)")
    plt.ylabel("CDF")
    plt.grid(True)
    plt.legend()
    plt.title("Latency CDF comparison")
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"[compare] wrote {out}")


if __name__ == "__main__":
    main()
