#!/usr/bin/env python3
"""
Benchmark driver for hamsaz-raft:
- Spawns N raft_node_cli processes (mix of seed/joiners)
- Supports simulated gossip or UDP gossip
- Sends a workload mix and records per-op latency + throughput
- Emits CSV + summary JSON + optional PNG plots into artifacts/bench/<timestamp>/
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List
import shutil
import random
import threading
import concurrent.futures
import socket

def now_ts():
    return time.strftime("%Y%m%d-%H%M%S")


def launch_node(build_dir, node_id, port, peers, join, gossip_udp, gossip_port, gossip_delay, gossip_drop, all_to_raft, args_inproc):
    args = [
        str(Path(build_dir) / "raft_node_cli"),
        "--id", str(node_id),
        "--port", str(port),
    ]
    if join:
        args.append("--join")
    for pid, host, pport in peers:
        args.extend(["--peer", f"{pid}:{host}:{pport}"])
    if gossip_udp:
        args.extend(["--gossip-udp", str(gossip_port)])
    if gossip_delay:
        args.extend(["--gossip-delay", gossip_delay])
    if gossip_drop is not None:
        args.extend(["--gossip-drop", str(gossip_drop)])
    if all_to_raft:
        args.append("--all-to-raft")
    if args_inproc:
        args.append("--inproc")
    proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    buffer: List[str] = []
    return proc, buffer


def read_until_prompt(proc, buf):
    """Block until we see a prompt '> ' at line start; accumulate output into buf list."""
    chunk = []
    while True:
        ch = proc.stdout.read(1)
        if ch == "" and proc.poll() is not None:
            break
        chunk.append(ch)
        if "".join(chunk).endswith("\n> "):
            break
    text = "".join(chunk)
    if text:
        buf.append(text)
    return text

def send_command(proc, buf, cmd):
    proc.stdin.write(cmd + "\n")
    proc.stdin.flush()
    return read_until_prompt(proc, buf)


def drain_to_eof(proc, buf, timeout=5):
    """Read remaining stdout until process exits or timeout."""
    start = time.time()
    while True:
        line = proc.stdout.readline()
        if line:
            buf.append(line)
        elif proc.poll() is not None:
            break
        if time.time() - start > timeout:
            break


def send_ops(procs, buffers, ops, targets, settle_ms, concurrency):
    """Send ops to possibly different nodes indicated by targets[i]."""
    # Support limited concurrency: multiple ops can be in-flight across nodes,
    # but we still serialize per-node sends to keep REPL I/O sane.
    latencies = []
    lat_lock = threading.Lock()
    node_locks = [threading.Lock() for _ in procs]
    primed = [False] * len(procs)
    alive = [i for i, p in enumerate(procs) if p.poll() is None]
    if not alive:
        raise RuntimeError("All nodes exited before workload")

    first_send = None
    last_finish = None
    last_finish_lock = threading.Lock()

    def do_one(idx):
        nonlocal first_send, last_finish, alive
        target = targets[idx]
        with node_locks[target]:
            if procs[target].poll() is not None:
                alive = [i for i, p in enumerate(procs) if p.poll() is None]
                if not alive:
                    return
                target = random.choice(alive)
            proc = procs[target]
            buf = buffers[target]
            if not primed[target]:
                read_until_prompt(proc, buf)
                primed[target] = True
            t0 = time.time()
            if first_send is None:
                first_send = t0
            try:
                proc.stdin.write(ops[idx] + "\n")
                proc.stdin.flush()
            except Exception:
                alive = [i for i, p in enumerate(procs) if p.poll() is None and i != target]
                if not alive:
                    return
                target = random.choice(alive)
                proc = procs[target]
                buf = buffers[target]
                if not primed[target]:
                    read_until_prompt(proc, buf)
                    primed[target] = True
                proc.stdin.write(ops[idx] + "\n")
                proc.stdin.flush()
            read_until_prompt(proc, buf)
            t1 = time.time()
            with lat_lock:
                latencies.append(t1 - t0)
            with last_finish_lock:
                last_finish = t1

    # Dispatch with a bounded worker pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        list(pool.map(do_one, range(len(ops))))

    if settle_ms > 0:
        time.sleep(settle_ms / 1000.0)
    for proc in procs:
        try:
            proc.stdin.write("exit\n")
            proc.stdin.flush()
        except Exception:
            pass
    wall = 0.0 if first_send is None or last_finish is None else last_finish - first_send
    return latencies, wall


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default="build", help="path to build dir")
    ap.add_argument("-n", "--nodes", type=int, default=3)
    ap.add_argument("--base-port", type=int, default=13000)
    ap.add_argument("--ops", type=int, default=100)
    ap.add_argument("--conflict-ratio", type=float, default=0.2, help="fraction of ops that are conflicting")
    ap.add_argument("--dependent-ratio", type=float, default=0.2, help="fraction of ops that are dependent (enroll)")
    ap.add_argument("--gossip-udp", action="store_true")
    ap.add_argument("--gossip-delay", default="2-8")
    ap.add_argument("--gossip-drop", type=float, default=0.0)
    ap.add_argument("--all-to-raft", action="store_true", help="force all ops through Raft (--all-to-raft flag on nodes)")
    ap.add_argument("--no-conflict-gate", action="store_true",
                    help="deprecated/no-op; conflict gate is always enabled")
    ap.add_argument("--out", default="artifacts/bench")
    ap.add_argument("--settle-ms", type=int, default=0, help="sleep after sending ops before exit")
    ap.add_argument("--concurrency", type=int, default=1, help="max in-flight client ops (>=1)")
    ap.add_argument("--no-port-probe", action="store_true", help="skip preflight bind checks (not recommended)")
    ap.add_argument("--inproc", action="store_true", help="start nodes with static in-process cluster config (no add_srv)")
    args = ap.parse_args()
    if args.no_conflict_gate:
        print("[bench] --no-conflict-gate is ignored; conflict gate is always enabled")

    Path(args.out).mkdir(parents=True, exist_ok=True)
    stamp = now_ts()
    run_dir = Path(args.out) / stamp
    run_dir.mkdir(parents=True, exist_ok=True)

    # Start each run from a clean slate so NuRaft configs don't clash across different
    # node counts or port ranges.
    for path in (Path("data"), Path("logs")):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    nodes = []
    buffers: List[List[str]] = []
    ports = [args.base_port + i for i in range(args.nodes)]
    udp_ports = [p + 1000 for p in ports]

    def check_port(host, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            s.close()
            return True
        except Exception:
            return False

    if not args.no_port_probe:
        for p in ports:
            if not check_port("127.0.0.1", p):
                print(f"[bench] ERROR: cannot bind tcp 127.0.0.1:{p}. Try a higher --base-port or run with permission to bind.")
                return 1
            if args.gossip_udp and not check_port("127.0.0.1", p + 1000):
                print(f"[bench] ERROR: cannot bind udp 127.0.0.1:{p+1000}. Try a higher --base-port.")
                return 1

    # launch seed first
    for i in range(args.nodes):
        node_id = i + 1
        # In inproc mode we don't need join skipping; everyone can elect immediately.
        join = False if args.inproc else (i != 0)
        peers = []
        for j in range(args.nodes):
            if j == i:
                continue
            peers.append((j + 1, "127.0.0.1", ports[j]))
        proc, buf = launch_node(args.build, node_id, ports[i], peers, join,
                               args.gossip_udp, udp_ports[i], args.gossip_delay,
                               args.gossip_drop, args.all_to_raft, args.inproc)
        nodes.append(proc)
        buffers.append(buf)
        time.sleep(1.0)

    print(f"[bench] launched {args.nodes} nodes")
    time.sleep(3)  # initial settle
    # Wait for seed to report full membership, up to 25s.
    start_wait = time.time()
    target_members = args.nodes
    while True:
        if nodes[0].poll() is not None:
            raise RuntimeError("seed node exited before membership check")
        text = send_command(nodes[0], buffers[0], "members")
        if text and "members" in text:
            try:
                parts = text.strip().split()
                count = int(parts[1])
                if count >= target_members:
                    break
            except Exception:
                pass
        if time.time() - start_wait > 25:
            raise RuntimeError("cluster membership not reached; aborting run")
        time.sleep(1)

    ops = []
    num_conflict = int(args.ops * args.conflict_ratio)
    num_dependent = int(args.ops * args.dependent_ratio)
    # We want deletes to have their prerequisites present; pre-create those courses.
    num_independent = args.ops - (num_conflict * 2 + num_dependent)
    if num_independent < 0:
        num_independent = 0

    # 1) Send deletes first so they queue (gate) when prereqs are missing.
    for i in range(num_conflict):
        ops.append(f"delete courseC{i}")
    # 2) Then add the courses to satisfy prerequisites; queued deletes should drain.
    for i in range(num_conflict):
        ops.append(f"add courseC{i}")
    # 3) Dependents (optionally reference existing courses).
    for i in range(num_dependent):
        base = num_conflict if num_conflict > 0 else 1
        ops.append(f"enroll student{i} courseC{i % base}")
    # 4) Remaining independent adds.
    for i in range(num_independent):
        ops.append(f"add courseI{i}")

    # Randomly target nodes for each op
    targets = [random.randrange(len(nodes)) for _ in ops]

    print(f"[bench] sending {len(ops)} ops (conflict ratio={args.conflict_ratio}) concurrency={args.concurrency}")
    latencies, wall_span = send_ops(nodes, buffers, ops, targets, args.settle_ms, args.concurrency)
    # drain outputs
    for idx, p in enumerate(nodes):
        drain_to_eof(p, buffers[idx], timeout=5)
        try:
            p.wait(timeout=2)
        except subprocess.TimeoutExpired:
            p.terminate()

    # write buffers to logs and parse raft append counts
    raft_appends = []
    gate_stats = {"queued": 0, "appended_from_queue": 0, "dropped": 0}
    copied_logs = []
    for idx, buf in enumerate(buffers, start=1):
        dest = run_dir / f"node{idx}.log"
        with open(dest, "w") as f:
            f.writelines(buf)
        copied_logs.append(str(dest))
        try:
            for line in buf:
                if "total raft appends:" in line:
                    parts = line.strip().split()
                    count = int(parts[-1])
                    raft_appends.append(count)
                if "conflict gate queued:" in line:
                    # format: conflict gate queued: X appended_from_queue: Y dropped: Z
                    tokens = line.strip().split()
                    # tokens[-5] should be queued value, etc.
                    gate_stats["queued"] += int(tokens[-5])
                    gate_stats["appended_from_queue"] += int(tokens[-3])
                    gate_stats["dropped"] += int(tokens[-1])
        except Exception:
            pass

    total = len(latencies)
    sum_lat = sum(latencies)
    avg = sum_lat / total if total else 0
    # Throughput definitions:
    # - rtt_throughput: assumes serialized client (sum of per-op latencies).
    # - wall_throughput: wall clock from first send to last completion (good when we add client concurrency).
    rtt_throughput = total / sum_lat if total and sum_lat > 0 else 0
    wall_throughput = total / wall_span if total and wall_span > 0 else 0

    csv_path = run_dir / "latencies.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["op_index", "latency_sec"])
        for i, l in enumerate(latencies):
            w.writerow([i, l])

    summary = {
        "nodes": args.nodes,
        "ops": total,
        "conflict_ratio": args.conflict_ratio,
        "dependent_ratio": args.dependent_ratio,
        "gossip_udp": args.gossip_udp,
        "gossip_drop": args.gossip_drop,
        "all_to_raft": args.all_to_raft,
        "conflict_gate": True,
        "avg_latency_sec": avg,
        "throughput_ops_per_sec": rtt_throughput,
        "wall_clock_span_sec": wall_span,
        "wall_throughput_ops_per_sec": wall_throughput,
        "logs": copied_logs,
        "raft_appends": raft_appends,
        "conflict_gate_stats": gate_stats,
    }
    with open(run_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"[bench] avg latency {avg:.4f}s throughput_rtt {rtt_throughput:.1f} ops/s wall_throughput {wall_throughput:.1f} ops/s span {wall_span:.4f}s")
    print(f"[bench] artifacts in {run_dir}")

    # plotting (optional)
    try:
        import matplotlib.pyplot as plt
        xs = list(range(total))
        plt.figure()
        plt.plot(xs, latencies, marker=".", linestyle="none", alpha=0.5)
        plt.xlabel("op index")
        plt.ylabel("latency (s)")
        plt.title("Per-op latency")
        plt.savefig(run_dir / "latency_scatter.png", dpi=150)
        plt.close()

        plt.figure()
        sorted_lat = sorted(latencies)
        ys = [i / total for i in range(total)]
        plt.plot(sorted_lat, ys)
        plt.xlabel("latency (s)")
        plt.ylabel("CDF")
        plt.title("Latency CDF")
        plt.grid(True)
        plt.savefig(run_dir / "latency_cdf.png", dpi=150)
        plt.close()
        print("[bench] plots written")
    except Exception as e:
        print(f"[bench] plotting skipped ({e})")

if __name__ == "__main__":
    sys.exit(main())
