#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""
Generate comparison plots for reproducible 3-node external runs.

Expected directory layout:
  <root>/
    split/summary.json
    split/latencies.csv
    all_to_raft/summary.json
    all_to_raft/latencies.csv

Output:
  <root>/latency_cdf_compare.png
  <root>/throughput_bar_compare.png
  <root>/avg_latency_bar_compare.png
  <root>/raft_appends_bar_compare.png
  <root>/gate_counters_bar_compare.png
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def load_latencies(csv_path: Path) -> list[float]:
    vals: list[float] = []
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                vals.append(float(row["latency_sec"]) * 1000.0)
            except Exception:
                continue
    vals.sort()
    return vals


def load_summary(path: Path) -> dict:
    return json.loads(path.read_text())


def cdf_y(n: int) -> list[float]:
    if n <= 0:
        return []
    return [i / n for i in range(1, n + 1)]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: plot_repro_compare.py <repro_root>")
        return 1

    root = Path(sys.argv[1]).resolve()
    split_dir = root / "split"
    raft_dir = root / "all_to_raft"

    split_sum = load_summary(split_dir / "summary.json")
    raft_sum = load_summary(raft_dir / "summary.json")
    split_lat = load_latencies(split_dir / "latencies.csv")
    raft_lat = load_latencies(raft_dir / "latencies.csv")

    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        print(f"[plot_repro_compare] plotting skipped ({exc})")
        return 0

    # 1) Common latency CDF
    plt.figure(figsize=(8, 5))
    plt.plot(split_lat, cdf_y(len(split_lat)), label="Split Routing", color="#1f77b4")
    plt.plot(raft_lat, cdf_y(len(raft_lat)), label="All-to-Raft", color="#ff7f0e")
    plt.xlabel("Latency (ms)")
    plt.ylabel("CDF")
    plt.title("Latency CDF: Split Routing vs All-to-Raft")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(root / "latency_cdf_compare.png", dpi=160)
    plt.close()

    # 2) Throughput comparison (RTT + wall)
    cases = ["Split Routing", "All-to-Raft"]
    x = [0, 1]
    width = 0.35
    split_rtt = float(split_sum.get("throughput_ops_per_sec", 0.0))
    raft_rtt = float(raft_sum.get("throughput_ops_per_sec", 0.0))
    split_wall = float(split_sum.get("wall_throughput_ops_per_sec", 0.0))
    raft_wall = float(raft_sum.get("wall_throughput_ops_per_sec", 0.0))

    plt.figure(figsize=(8, 5))
    plt.bar([i - width / 2 for i in x], [split_rtt, raft_rtt], width=width, label="RTT throughput")
    plt.bar([i + width / 2 for i in x], [split_wall, raft_wall], width=width, label="Wall throughput")
    plt.xticks(x, cases)
    plt.ylabel("Ops / sec")
    plt.title("Throughput Comparison")
    plt.grid(True, axis="y", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(root / "throughput_bar_compare.png", dpi=160)
    plt.close()

    # 3) Average latency bar
    split_avg = float(split_sum.get("avg_latency_sec", 0.0)) * 1000.0
    raft_avg = float(raft_sum.get("avg_latency_sec", 0.0)) * 1000.0
    plt.figure(figsize=(7, 5))
    plt.bar(cases, [split_avg, raft_avg], color=["#1f77b4", "#ff7f0e"])
    plt.ylabel("Average latency (ms)")
    plt.title("Average Latency Comparison")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(root / "avg_latency_bar_compare.png", dpi=160)
    plt.close()

    # 4) Raft append pressure
    split_app = int(sum(split_sum.get("raft_appends", [])))
    raft_app = int(sum(raft_sum.get("raft_appends", [])))
    plt.figure(figsize=(7, 5))
    plt.bar(cases, [split_app, raft_app], color=["#1f77b4", "#ff7f0e"])
    plt.ylabel("Total Raft appends")
    plt.title("Raft Log Pressure Comparison")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(root / "raft_appends_bar_compare.png", dpi=160)
    plt.close()

    # 5) Gate counters comparison
    sg = split_sum.get("conflict_gate_stats", {})
    rg = raft_sum.get("conflict_gate_stats", {})
    metrics = ["queued", "appended_from_queue", "dropped"]
    split_vals = [int(sg.get(k, 0)) for k in metrics]
    raft_vals = [int(rg.get(k, 0)) for k in metrics]
    x2 = list(range(len(metrics)))

    plt.figure(figsize=(8, 5))
    plt.bar([i - width / 2 for i in x2], split_vals, width=width, label="Split Routing")
    plt.bar([i + width / 2 for i in x2], raft_vals, width=width, label="All-to-Raft")
    plt.xticks(x2, ["Queued", "Appended", "Dropped"])
    plt.ylabel("Count")
    plt.title("Conflict Gate Counters")
    plt.grid(True, axis="y", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(root / "gate_counters_bar_compare.png", dpi=160)
    plt.close()

    print(f"[plot_repro_compare] wrote plots under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

