#!/usr/bin/env python3
"""
Strict benchmark matrix runner.

Runs bench_multiclient.py across a config matrix, enforces correctness checks,
aggregates run metrics, and emits summary CSV/JSON plus comparison plots.
"""

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


def now_ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def parse_int_list(raw: str) -> List[int]:
    out = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    if not out:
        raise ValueError(f"empty integer list: {raw}")
    return out


def parse_policy_list(raw: str) -> List[str]:
    allowed = {"split", "all_to_raft"}
    out = []
    for token in raw.split(","):
        token = token.strip().lower()
        if not token:
            continue
        if token not in allowed:
            raise ValueError(f"unsupported policy '{token}', allowed={sorted(allowed)}")
        out.append(token)
    if not out:
        raise ValueError("policy list is empty")
    return out


def newest_summary(path: Path) -> Optional[Path]:
    candidates = sorted(path.glob("*/summary.json"), key=lambda p: p.stat().st_mtime)
    return candidates[-1] if candidates else None


def safe_mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return statistics.mean(values)


def safe_std(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return statistics.pstdev(values)


def as_number(value):
    return "" if value is None else value


def parse_summary(summary: Dict) -> Dict:
    correctness = summary.get("correctness") or {}
    drain = summary.get("drain") or {}
    gate = summary.get("conflict_gate_stats") or {}
    raft_appends = summary.get("raft_appends") or []
    return {
        "avg_latency_ms": float(summary.get("avg_latency_sec", 0.0)) * 1000.0,
        "rtt_throughput_ops_sec": float(summary.get("throughput_ops_per_sec", 0.0)),
        "wall_throughput_ops_sec": float(summary.get("wall_throughput_ops_per_sec", 0.0)),
        "op_failures": int(summary.get("op_failures", 0)),
        "correct_converged": bool(correctness.get("converged", False)),
        "correct_all_invariants": bool(correctness.get("all_invariants_ok", False)),
        "correct_settle_sec": float(correctness.get("settle_time_sec", 0.0)) if correctness else None,
        "drain_drained": bool(drain.get("drained", False)) if drain else None,
        "drain_pending": int(drain.get("pending", 0)) if drain else None,
        "drain_settle_sec": float(drain.get("settle_time_sec", 0.0)) if drain else None,
        "gate_queued": int(gate.get("queued", 0)),
        "gate_appended": int(gate.get("appended_from_queue", 0)),
        "gate_dropped": int(gate.get("dropped", 0)),
        "raft_appends_sum": int(sum(raft_appends)),
    }


def run_one(
    args,
    out_root: Path,
    run_index: int,
    policy: str,
    nodes: int,
    ops: int,
    concurrency: int,
    trial: int,
    attempt: int,
):
    label = f"{policy}_n{nodes}_o{ops}_c{concurrency}_t{trial}_a{attempt}"
    per_case_out = out_root / "raw" / label
    per_case_out.mkdir(parents=True, exist_ok=True)
    log_dir = out_root / "command_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cmd_log = log_dir / f"{label}.log"

    # Use non-overlapping port ranges across runs to avoid transient bind races.
    port_offset = run_index * args.port_stride
    raft_base = args.base_port + port_offset
    api_base = args.api_base_port + port_offset

    cmd = [
        sys.executable,
        "bench/bench_multiclient.py",
        "--build", str(args.build),
        "--nodes", str(nodes),
        "--ops", str(ops),
        "--conflict-ratio", str(args.conflict_ratio),
        "--dependent-ratio", str(args.dependent_ratio),
        "--concurrency", str(concurrency),
        "--base-port", str(raft_base),
        "--api-base-port", str(api_base),
        "--max-retries", str(args.max_retries),
        "--retry-delay-ms", str(args.retry_delay_ms),
        "--request-timeout-sec", str(args.request_timeout_sec),
        "--drain-timeout-sec", str(args.drain_timeout_sec),
        "--drain-poll-ms", str(args.drain_poll_ms),
        "--settle-timeout-sec", str(args.settle_timeout_sec),
        "--settle-poll-ms", str(args.settle_poll_ms),
        "--verify-correctness",
        "--fail-on-correctness",
        "--fail-on-op-errors",
        "--out", str(per_case_out),
    ]
    if args.inproc:
        cmd.append("--inproc")
    if args.gossip_sim_only:
        cmd.append("--gossip-sim-only")
    if policy == "all_to_raft":
        cmd.append("--all-to-raft")

    started_at = time.time()
    proc = subprocess.run(cmd, text=True, capture_output=True)
    duration_sec = time.time() - started_at
    with cmd_log.open("w") as f:
        f.write("$ " + " ".join(cmd) + "\n\n")
        f.write(proc.stdout)
        if proc.stderr:
            f.write("\n[stderr]\n")
            f.write(proc.stderr)

    summary_path = newest_summary(per_case_out)
    row = {
        "label": label,
        "policy": policy,
        "nodes": nodes,
        "ops": ops,
        "concurrency": concurrency,
        "trial": trial,
        "attempt": attempt,
        "return_code": proc.returncode,
        "duration_sec": duration_sec,
        "summary_path": str(summary_path) if summary_path else "",
    }

    if summary_path and summary_path.exists():
        with summary_path.open() as f:
            summary = json.load(f)
        row.update(parse_summary(summary))
    else:
        row.update({
            "avg_latency_ms": None,
            "rtt_throughput_ops_sec": None,
            "wall_throughput_ops_sec": None,
            "op_failures": None,
            "correct_converged": None,
            "correct_all_invariants": None,
            "correct_settle_sec": None,
            "drain_drained": None,
            "drain_pending": None,
            "drain_settle_sec": None,
            "gate_queued": None,
            "gate_appended": None,
            "gate_dropped": None,
            "raft_appends_sum": None,
        })
    return row


def is_pass(row: Dict) -> bool:
    return (
        row.get("return_code") == 0
        and row.get("op_failures") == 0
        and row.get("correct_converged") is True
        and row.get("correct_all_invariants") is True
        and row.get("drain_drained") is True
    )


def write_runs_csv(path: Path, rows: List[Dict]):
    fieldnames = [
        "label", "policy", "nodes", "ops", "concurrency", "trial", "attempt",
        "return_code", "duration_sec", "summary_path",
        "avg_latency_ms", "rtt_throughput_ops_sec", "wall_throughput_ops_sec",
        "op_failures",
        "correct_converged", "correct_all_invariants", "correct_settle_sec",
        "drain_drained", "drain_pending", "drain_settle_sec",
        "gate_queued", "gate_appended", "gate_dropped",
        "raft_appends_sum",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def summarize(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[str, int, int, int], List[Dict]] = {}
    for row in rows:
        key = (row["policy"], row["nodes"], row["ops"], row["concurrency"])
        grouped.setdefault(key, []).append(row)

    out = []
    for key, group in sorted(grouped.items(), key=lambda x: x[0]):
        policy, nodes, ops, concurrency = key
        pass_count = sum(1 for r in group if is_pass(r))
        total = len(group)
        avg_latency_vals = [float(r["avg_latency_ms"]) for r in group if r.get("avg_latency_ms") is not None]
        wall_vals = [float(r["wall_throughput_ops_sec"]) for r in group if r.get("wall_throughput_ops_sec") is not None]
        rtt_vals = [float(r["rtt_throughput_ops_sec"]) for r in group if r.get("rtt_throughput_ops_sec") is not None]
        raft_vals = [float(r["raft_appends_sum"]) for r in group if r.get("raft_appends_sum") is not None]
        settle_vals = [float(r["correct_settle_sec"]) for r in group if r.get("correct_settle_sec") is not None]
        queued_vals = [float(r["gate_queued"]) for r in group if r.get("gate_queued") is not None]
        dropped_vals = [float(r["gate_dropped"]) for r in group if r.get("gate_dropped") is not None]
        op_fail_vals = [float(r["op_failures"]) for r in group if r.get("op_failures") is not None]

        out.append({
            "policy": policy,
            "nodes": nodes,
            "ops": ops,
            "concurrency": concurrency,
            "trials": total,
            "pass_count": pass_count,
            "pass_rate": (pass_count / total) if total else 0.0,
            "avg_latency_ms_mean": safe_mean(avg_latency_vals),
            "avg_latency_ms_std": safe_std(avg_latency_vals),
            "wall_throughput_mean": safe_mean(wall_vals),
            "wall_throughput_std": safe_std(wall_vals),
            "rtt_throughput_mean": safe_mean(rtt_vals),
            "rtt_throughput_std": safe_std(rtt_vals),
            "raft_appends_sum_mean": safe_mean(raft_vals),
            "raft_appends_sum_std": safe_std(raft_vals),
            "correct_settle_sec_mean": safe_mean(settle_vals),
            "gate_queued_mean": safe_mean(queued_vals),
            "gate_dropped_mean": safe_mean(dropped_vals),
            "op_failures_mean": safe_mean(op_fail_vals),
        })
    return out


def write_summary_csv(path: Path, rows: List[Dict]):
    fieldnames = [
        "policy", "nodes", "ops", "concurrency",
        "trials", "pass_count", "pass_rate",
        "avg_latency_ms_mean", "avg_latency_ms_std",
        "wall_throughput_mean", "wall_throughput_std",
        "rtt_throughput_mean", "rtt_throughput_std",
        "raft_appends_sum_mean", "raft_appends_sum_std",
        "correct_settle_sec_mean",
        "gate_queued_mean", "gate_dropped_mean", "op_failures_mean",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            out = {k: as_number(v) for k, v in row.items()}
            w.writerow(out)


def plot_summary(summary_rows: List[Dict], out_dir: Path):
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        print(f"[matrix] plotting skipped ({exc})")
        return

    if not summary_rows:
        return

    def plot_metric(metric_key: str, ylabel: str, filename: str):
        plt.figure(figsize=(10, 6))
        grouped: Dict[Tuple[str, int, int], List[Tuple[int, float]]] = {}
        for row in summary_rows:
            val = row.get(metric_key)
            if val is None:
                continue
            label_key = (row["policy"], row["nodes"], row["ops"])
            grouped.setdefault(label_key, []).append((row["concurrency"], float(val)))

        for (policy, nodes, ops), points in sorted(grouped.items()):
            points = sorted(points, key=lambda x: x[0])
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            label = f"{policy} n={nodes} o={ops}"
            plt.plot(xs, ys, marker="o", label=label)

        plt.xlabel("Concurrency (in-flight requests)")
        plt.ylabel(ylabel)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_dir / filename, dpi=150)
        plt.close()

    plot_metric("wall_throughput_mean", "Wall throughput (ops/s)", "wall_throughput_vs_concurrency.png")
    plot_metric("avg_latency_ms_mean", "Average latency (ms)", "avg_latency_vs_concurrency.png")
    plot_metric("raft_appends_sum_mean", "Total Raft appends", "raft_appends_vs_concurrency.png")
    plot_metric("pass_rate", "Correctness pass rate", "pass_rate_vs_concurrency.png")


def write_report(path: Path, args, run_count: int, pass_count: int, summary_rows: List[Dict]):
    def fmt(value, precision: int = 3) -> str:
        if value is None:
            return "n/a"
        return f"{float(value):.{precision}f}"

    lines = []
    lines.append("# Strict Benchmark Matrix Report")
    lines.append("")
    lines.append(f"- Generated: `{time.strftime('%Y-%m-%d %H:%M:%S')}`")
    lines.append(f"- Total runs: `{run_count}`")
    lines.append(f"- Passing runs: `{pass_count}`")
    lines.append("")
    lines.append("## Matrix")
    lines.append("")
    lines.append(f"- Policies: `{args.policies}`")
    lines.append(f"- Nodes: `{args.nodes}`")
    lines.append(f"- Ops: `{args.ops}`")
    lines.append(f"- Concurrency: `{args.concurrency}`")
    lines.append(f"- Trials per config: `{args.trials}`")
    lines.append("")
    lines.append("## Strict correctness guardrails")
    lines.append("")
    lines.append("- `--verify-correctness`")
    lines.append("- `--fail-on-correctness`")
    lines.append("- `--fail-on-op-errors`")
    lines.append("- Queue drain before convergence check")
    lines.append("")
    lines.append("## Output files")
    lines.append("")
    lines.append("- `runs.csv`")
    lines.append("- `summary_by_config.csv`")
    lines.append("- `summary_by_config.json`")
    lines.append("- `wall_throughput_vs_concurrency.png`")
    lines.append("- `avg_latency_vs_concurrency.png`")
    lines.append("- `raft_appends_vs_concurrency.png`")
    lines.append("- `pass_rate_vs_concurrency.png`")
    lines.append("")
    if summary_rows:
        top = sorted(
            summary_rows,
            key=lambda r: (
                -float(r["pass_rate"]),
                -(float(r["wall_throughput_mean"]) if r["wall_throughput_mean"] is not None else -1.0),
            ),
        )[:5]
        lines.append("## Top configs by correctness then throughput")
        lines.append("")
        for row in top:
            lines.append(
                f"- `{row['policy']}` n={row['nodes']} o={row['ops']} c={row['concurrency']}: "
                f"pass_rate={row['pass_rate']:.2f}, "
                f"wall_tput={fmt(row['wall_throughput_mean'], 1)} ops/s, "
                f"lat={fmt(row['avg_latency_ms_mean'], 3)} ms, "
                f"raft={fmt(row['raft_appends_sum_mean'], 1)}"
            )
        lines.append("")
    path.write_text("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default="build")
    ap.add_argument("--out", default="artifacts/bench_matrix")
    ap.add_argument("--nodes", default="10,20")
    ap.add_argument("--ops", default="1500,5000")
    ap.add_argument("--concurrency", default="8,32,64")
    ap.add_argument("--policies", default="split",
                    help="comma-separated: split,all_to_raft")
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--conflict-ratio", type=float, default=0.25)
    ap.add_argument("--dependent-ratio", type=float, default=0.25)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--retry-delay-ms", type=int, default=10)
    ap.add_argument("--request-timeout-sec", type=float, default=8.0)
    ap.add_argument("--run-retries", type=int, default=2,
                    help="benchmark process retries per logical trial if strict checks fail")
    ap.add_argument("--drain-timeout-sec", type=float, default=6.0)
    ap.add_argument("--drain-poll-ms", type=int, default=200)
    ap.add_argument("--settle-timeout-sec", type=float, default=60.0)
    ap.add_argument("--settle-poll-ms", type=int, default=200)
    ap.add_argument("--base-port", type=int, default=15000)
    ap.add_argument("--api-base-port", type=int, default=25000)
    ap.add_argument("--port-stride", type=int, default=200)
    ap.add_argument("--cooldown-sec", type=float, default=0.5)
    ap.add_argument("--inproc", action="store_true")
    ap.add_argument("--gossip-sim-only", action="store_true")
    ap.add_argument("--continue-on-failure", action="store_true")
    args = ap.parse_args()

    nodes = parse_int_list(args.nodes)
    ops_list = parse_int_list(args.ops)
    conc_list = parse_int_list(args.concurrency)
    policies = parse_policy_list(args.policies)

    run_root = Path(args.out) / now_ts()
    run_root.mkdir(parents=True, exist_ok=True)
    print(f"[matrix] output root: {run_root}")

    rows: List[Dict] = []
    run_index = 0
    logical_index = 0
    for policy in policies:
        for n in nodes:
            for ops in ops_list:
                for conc in conc_list:
                    for trial in range(1, args.trials + 1):
                        logical_index += 1
                        print(
                            f"[matrix] run {logical_index}: policy={policy} n={n} ops={ops} "
                            f"conc={conc} trial={trial}"
                        )
                        row = None
                        for attempt in range(1, max(1, args.run_retries) + 1):
                            run_index += 1
                            row = run_one(
                                args, run_root, run_index, policy, n, ops, conc, trial, attempt
                            )
                            ok = is_pass(row)
                            print(
                                f"[matrix]   attempt={attempt} rc={row['return_code']} pass={ok} "
                                f"lat_ms={row.get('avg_latency_ms')} "
                                f"wall_tput={row.get('wall_throughput_ops_sec')}"
                            )
                            if ok:
                                break
                            if attempt < max(1, args.run_retries):
                                print("[matrix]   retrying logical trial after failure")
                                time.sleep(max(0.0, args.cooldown_sec))
                        assert row is not None
                        rows.append(row)
                        ok = is_pass(row)
                        if not ok and not args.continue_on_failure:
                            print("[matrix] stopping on first failed run (use --continue-on-failure to keep going)")
                            break
                        time.sleep(max(0.0, args.cooldown_sec))
                    if rows and not is_pass(rows[-1]) and not args.continue_on_failure:
                        break
                if rows and not is_pass(rows[-1]) and not args.continue_on_failure:
                    break
            if rows and not is_pass(rows[-1]) and not args.continue_on_failure:
                break
        if rows and not is_pass(rows[-1]) and not args.continue_on_failure:
            break

    runs_csv = run_root / "runs.csv"
    write_runs_csv(runs_csv, rows)
    summary_rows = summarize(rows)
    summary_csv = run_root / "summary_by_config.csv"
    write_summary_csv(summary_csv, summary_rows)
    with (run_root / "summary_by_config.json").open("w") as f:
        json.dump(summary_rows, f, indent=2)
    plot_summary(summary_rows, run_root)
    pass_count = sum(1 for row in rows if is_pass(row))
    write_report(run_root / "README.md", args, len(rows), pass_count, summary_rows)

    print(f"[matrix] runs.csv: {runs_csv}")
    print(f"[matrix] summary_by_config.csv: {summary_csv}")
    print(f"[matrix] pass_count={pass_count}/{len(rows)}")
    return 0 if (pass_count == len(rows)) else 2


if __name__ == "__main__":
    raise SystemExit(main())
