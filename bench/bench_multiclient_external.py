#!/usr/bin/env python3
"""
External true multi-client benchmark:
- Targets already-running raft_node_server endpoints over TCP.
- Does not spawn or manage server processes.
- Useful for Docker Compose / Kubernetes / multi-host testing.
"""

import argparse
import csv
import json
import random
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple


def now_ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


class NodeConn:
    def __init__(self, host: str, port: int, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.file = None
        self._connect()

    def _connect(self):
        self.close()
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.file = self.sock.makefile("rwb", buffering=0)

    def close(self):
        try:
            if self.file:
                self.file.close()
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.file = None
        self.sock = None

    def request(self, line: str) -> str:
        payload = (line + "\n").encode("utf-8")
        try:
            self.file.write(payload)
            self.file.flush()
            resp = self.file.readline()
            if not resp:
                raise RuntimeError("empty response")
            return resp.decode("utf-8", errors="replace").rstrip("\r\n")
        except Exception:
            self._connect()
            self.file.write(payload)
            self.file.flush()
            resp = self.file.readline()
            if not resp:
                raise RuntimeError("empty response after reconnect")
            return resp.decode("utf-8", errors="replace").rstrip("\r\n")


def parse_hosts(raw: str) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"invalid host entry: {item}")
        host, port_s = item.rsplit(":", 1)
        out.append((host, int(port_s)))
    if not out:
        raise ValueError("no hosts provided")
    return out


def parse_members(resp: str) -> int:
    parts = resp.split("\t")
    if len(parts) == 2 and parts[0] == "MEMBERS":
        return int(parts[1])
    return -1


def parse_stats(resp: str) -> Tuple[int, int, int, int]:
    # STATS \t raft_appends \t queued \t appended_from_queue \t dropped
    parts = resp.split("\t")
    if len(parts) != 5 or parts[0] != "STATS":
        raise RuntimeError(f"bad STATS response: {resp}")
    return int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])


def parse_invariant(resp: str) -> bool:
    parts = resp.split("\t")
    if len(parts) != 2 or parts[0] != "INVARIANT":
        raise RuntimeError(f"bad INVARIANT response: {resp}")
    return parts[1] == "1"


def parse_state_hash(resp: str) -> Dict[str, object]:
    # STATE_HASH \t hash \t students \t courses \t enrollments
    parts = resp.split("\t")
    if len(parts) != 5 or parts[0] != "STATE_HASH":
        raise RuntimeError(f"bad STATE_HASH response: {resp}")
    return {
        "hash": parts[1],
        "students": int(parts[2]),
        "courses": int(parts[3]),
        "enrollments": int(parts[4]),
    }


def build_workload(ops: int, conflict_ratio: float, dependent_ratio: float) -> List[str]:
    num_conflict = int(ops * conflict_ratio)
    num_dependent = int(ops * dependent_ratio)
    num_independent = ops - (num_conflict * 2 + num_dependent)
    if num_independent < 0:
        num_independent = 0

    out: List[str] = []
    for i in range(num_conflict):
        out.append(f"delete\tcourseC{i}\t_")
    for i in range(num_conflict):
        out.append(f"add\tcourseC{i}\t_")
    base = num_conflict if num_conflict > 0 else 1
    for i in range(num_dependent):
        out.append(f"enroll\tstudent{i}\tcourseC{i % base}")
    for i in range(num_independent):
        out.append(f"add\tcourseI{i}\t_")
    return out


def wait_cluster_ready(endpoints: List[Tuple[str, int]], expected_members: int, timeout_sec: float):
    c = NodeConn(endpoints[0][0], endpoints[0][1], timeout=2.0)
    try:
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            try:
                if c.request("PING") != "PONG":
                    time.sleep(0.2)
                    continue
                m = parse_members(c.request("MEMBERS"))
                if m >= expected_members:
                    return
            except Exception:
                pass
            time.sleep(0.5)
    finally:
        c.close()
    raise RuntimeError("cluster membership not reached")


def run_workload(endpoints: List[Tuple[str, int]], ops: List[str], concurrency: int, op_prefix: str,
                 max_retries: int, retry_delay_ms: int, fail_on_op_errors: bool,
                 request_timeout_sec: float):
    thread_local = threading.local()
    latencies: List[float] = []
    lat_mu = threading.Lock()
    fail_mu = threading.Lock()
    all_conns: List[NodeConn] = []
    conn_mu = threading.Lock()
    failures = 0
    attempts_total = 0
    attempts_mu = threading.Lock()
    time_mu = threading.Lock()
    first_send = None
    last_finish = None

    targets = [random.randrange(len(endpoints)) for _ in ops]

    def get_conn(idx: int) -> NodeConn:
        conn_map: Dict[int, NodeConn] = getattr(thread_local, "conn_map", None)
        if conn_map is None:
            conn_map = {}
            thread_local.conn_map = conn_map
        c = conn_map.get(idx)
        if c is None:
            h, p = endpoints[idx]
            c = NodeConn(h, p, timeout=request_timeout_sec)
            conn_map[idx] = c
            with conn_mu:
                all_conns.append(c)
        return c

    def one(i: int):
        nonlocal first_send, last_finish, failures, attempts_total
        idx = targets[i]
        cmd, a1, a2 = ops[i].split("\t")
        line = f"EXEC\t{op_prefix}-op-{i+1}\t{cmd}\t{a1}\t{a2}"
        c = get_conn(idx)
        max_attempts = max(1, max_retries + 1)
        op_start = time.time()
        for attempt in range(max_attempts):
            with attempts_mu:
                attempts_total += 1
            t0 = time.time()
            with time_mu:
                if first_send is None:
                    first_send = t0
            resp = c.request(line)

            parts = resp.split("\t", 2)
            if len(parts) < 2 or parts[0] != "RES":
                raise RuntimeError(f"bad response: {resp}")
            ok = parts[1] == "1"
            msg = parts[2] if len(parts) >= 3 else ""
            if ok or msg.startswith("Deferred"):
                t_done = time.time()
                with time_mu:
                    last_finish = t_done
                with lat_mu:
                    # Per-op end-to-end latency, including any retries.
                    latencies.append(t_done - op_start)
                return
            if attempt + 1 >= max_attempts:
                t_done = time.time()
                with time_mu:
                    last_finish = t_done
                with lat_mu:
                    latencies.append(t_done - op_start)
                if fail_on_op_errors:
                    raise RuntimeError(f"operation failed after retries: {resp}")
                with fail_mu:
                    failures += 1
                return
            # Followers in older builds may reject conflicting writes.
            # Retry by re-targeting another endpoint (likely leader).
            lower_msg = msg.lower()
            if "raft append rejected" in lower_msg or "not leader" in lower_msg:
                idx = random.randrange(len(endpoints))
                c = get_conn(idx)
            time.sleep(max(0, retry_delay_ms) / 1000.0)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        list(pool.map(one, range(len(ops))))

    for c in all_conns:
        c.close()

    wall = 0.0 if first_send is None or last_finish is None else (last_finish - first_send)
    return latencies, wall, failures, attempts_total


def fetch_stats(endpoints: List[Tuple[str, int]]):
    raft_appends: List[int] = []
    gate = {"queued": 0, "appended_from_queue": 0, "dropped": 0}
    for host, port in endpoints:
        c = NodeConn(host, port, timeout=2.0)
        try:
            ra, q, aq, dr = parse_stats(c.request("STATS"))
            raft_appends.append(ra)
            gate["queued"] += q
            gate["appended_from_queue"] += aq
            gate["dropped"] += dr
        finally:
            c.close()
    return raft_appends, gate


def verify_correctness(endpoints: List[Tuple[str, int]],
                       settle_timeout_sec: float,
                       settle_poll_ms: int):
    t0 = time.time()
    attempts = 0
    last_state = None
    while True:
        attempts += 1
        nodes = []
        for i, (host, port) in enumerate(endpoints, start=1):
            c = NodeConn(host, port, timeout=2.0)
            try:
                invariant_ok = parse_invariant(c.request("INVARIANT"))
                state = parse_state_hash(c.request("STATE_HASH"))
                nodes.append({
                    "node": i,
                    "endpoint": f"{host}:{port}",
                    "invariant_ok": invariant_ok,
                    **state,
                })
            finally:
                c.close()

        hashes = {n["hash"] for n in nodes}
        all_invariants = all(n["invariant_ok"] for n in nodes)
        converged = len(hashes) == 1 and all_invariants
        elapsed = time.time() - t0
        last_state = {
            "converged": converged,
            "all_invariants_ok": all_invariants,
            "unique_hashes": sorted(hashes),
            "settle_time_sec": elapsed,
            "attempts": attempts,
            "nodes": nodes,
        }
        if converged:
            return last_state
        if elapsed >= settle_timeout_sec:
            return last_state
        time.sleep(max(1, settle_poll_ms) / 1000.0)


def pending_conflicts(gate: Dict[str, int]) -> int:
    return max(0, gate["queued"] - gate["appended_from_queue"] - gate["dropped"])


def drain_conflict_queue(endpoints: List[Tuple[str, int]], timeout_sec: float, poll_ms: int):
    t0 = time.time()
    attempts = 0
    _, first_gate = fetch_stats(endpoints)
    while True:
        attempts += 1
        _, gate = fetch_stats(endpoints)
        pending = pending_conflicts(gate)
        elapsed = time.time() - t0
        if pending == 0:
            return {
                "drained": True,
                "pending": pending,
                "settle_time_sec": elapsed,
                "attempts": attempts,
                "start_gate": first_gate,
                "end_gate": gate,
            }
        if elapsed >= timeout_sec:
            return {
                "drained": False,
                "pending": pending,
                "settle_time_sec": elapsed,
                "attempts": attempts,
                "start_gate": first_gate,
                "end_gate": gate,
            }
        time.sleep(max(1, poll_ms) / 1000.0)


def shutdown_nodes(endpoints: List[Tuple[str, int]]):
    for host, port in endpoints:
        c = NodeConn(host, port, timeout=2.0)
        try:
            c.request("SHUTDOWN")
        except Exception:
            pass
        finally:
            c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", required=True,
                    help="comma-separated host:port list, e.g. 127.0.0.1:25001,127.0.0.1:25002")
    ap.add_argument("--ops", type=int, default=1500)
    ap.add_argument("--conflict-ratio", type=float, default=0.25)
    ap.add_argument("--dependent-ratio", type=float, default=0.25)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--wait-members", type=int, default=0,
                    help="expected cluster membership (default: number of endpoints)")
    ap.add_argument("--wait-timeout-sec", type=float, default=30.0)
    ap.add_argument("--verify-correctness", action="store_true",
                    help="verify convergence (state hash + invariants) after workload")
    ap.add_argument("--settle-timeout-sec", type=float, default=8.0,
                    help="max time to wait for convergence checks")
    ap.add_argument("--settle-poll-ms", type=int, default=200,
                    help="poll interval for convergence checks")
    ap.add_argument("--drain-timeout-sec", type=float, default=3.0,
                    help="max time to wait for queued conflict ops to drain before correctness check")
    ap.add_argument("--drain-poll-ms", type=int, default=200,
                    help="poll interval for queue-drain checks")
    ap.add_argument("--fail-on-correctness", action="store_true",
                    help="exit non-zero if convergence/invariant check fails")
    ap.add_argument("--max-retries", type=int, default=2,
                    help="max retries for explicit RES\\t0 failures (Deferred is not retried)")
    ap.add_argument("--retry-delay-ms", type=int, default=10,
                    help="delay between retries for failed operations")
    ap.add_argument("--request-timeout-sec", type=float, default=5.0,
                    help="per-request socket timeout for benchmark clients")
    ap.add_argument("--fail-on-op-errors", action="store_true",
                    help="abort run if an operation still returns RES\\t0 after retries")
    ap.add_argument("--shutdown-after", action="store_true",
                    help="send SHUTDOWN to all endpoints when benchmark completes")
    ap.add_argument("--out", default="artifacts/bench_external")
    args = ap.parse_args()

    endpoints = parse_hosts(args.hosts)
    expected_members = args.wait_members if args.wait_members > 0 else len(endpoints)

    run_dir = Path(args.out) / now_ts()
    run_dir.mkdir(parents=True, exist_ok=True)

    wait_cluster_ready(endpoints, expected_members, args.wait_timeout_sec)
    ops = build_workload(args.ops, args.conflict_ratio, args.dependent_ratio)
    print(f"[bench_ext] sending {len(ops)} ops concurrency={args.concurrency} endpoints={len(endpoints)}")
    op_prefix = f"ext-{now_ts()}-{random.randrange(1_000_000)}"
    latencies, wall_span, op_failures, attempts_total = run_workload(
        endpoints, ops, args.concurrency, op_prefix,
        args.max_retries, args.retry_delay_ms, args.fail_on_op_errors,
        args.request_timeout_sec
    )
    raft_appends, gate_stats = fetch_stats(endpoints)
    correctness = None
    drain_info = None
    if args.verify_correctness:
        drain_info = drain_conflict_queue(endpoints, args.drain_timeout_sec, args.drain_poll_ms)
        correctness = verify_correctness(endpoints, args.settle_timeout_sec, args.settle_poll_ms)

    total = len(latencies)
    sum_lat = sum(latencies)
    avg = sum_lat / total if total else 0.0
    rtt_throughput = total / sum_lat if total and sum_lat > 0 else 0.0
    wall_throughput = total / wall_span if total and wall_span > 0 else 0.0

    with open(run_dir / "latencies.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["op_index", "latency_sec"])
        for i, l in enumerate(latencies):
            w.writerow([i, l])

    summary = {
        "nodes": len(endpoints),
        "ops": total,
        "conflict_ratio": args.conflict_ratio,
        "dependent_ratio": args.dependent_ratio,
        "concurrency": args.concurrency,
        "conflict_gate": True,
        "avg_latency_sec": avg,
        "throughput_ops_per_sec": rtt_throughput,
        "wall_clock_span_sec": wall_span,
        "wall_throughput_ops_per_sec": wall_throughput,
        "raft_appends": raft_appends,
        "conflict_gate_stats": gate_stats,
        "drain": drain_info,
        "endpoints": [f"{h}:{p}" for h, p in endpoints],
        "correctness": correctness,
        "op_failures": op_failures,
        "attempts_total": attempts_total,
        "retries_total": max(0, attempts_total - len(ops)),
    }
    with open(run_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"[bench_ext] avg latency {avg:.6f}s throughput_rtt {rtt_throughput:.1f} wall_throughput {wall_throughput:.1f}")
    print(f"[bench_ext] op_failures={op_failures}")
    if drain_info:
        print(f"[bench_ext] drain drained={drain_info['drained']} pending={drain_info['pending']} "
              f"settle={drain_info['settle_time_sec']:.3f}s")
    if correctness:
        print(f"[bench_ext] correctness converged={correctness['converged']} "
              f"invariants={correctness['all_invariants_ok']} "
              f"settle={correctness['settle_time_sec']:.3f}s")
    print(f"[bench_ext] artifacts: {run_dir}")

    try:
        import matplotlib.pyplot as plt
        xs = list(range(total))
        plt.figure()
        plt.plot(xs, latencies, marker=".", linestyle="none", alpha=0.4)
        plt.xlabel("op index")
        plt.ylabel("latency (s)")
        plt.title("Per-op latency (external multi-client)")
        plt.savefig(run_dir / "latency_scatter.png", dpi=150)
        plt.close()

        plt.figure()
        sl = sorted(latencies)
        ys = [i / total for i in range(total)]
        plt.plot(sl, ys)
        plt.xlabel("latency (s)")
        plt.ylabel("CDF")
        plt.title("Latency CDF (external multi-client)")
        plt.grid(True)
        plt.savefig(run_dir / "latency_cdf.png", dpi=150)
        plt.close()
        print("[bench_ext] plots written")
    except Exception as e:
        print(f"[bench_ext] plotting skipped ({e})")

    if args.shutdown_after:
        shutdown_nodes(endpoints)
        print("[bench_ext] sent SHUTDOWN to all endpoints")

    if args.fail_on_correctness and correctness and not correctness["converged"]:
        print("[bench_ext] correctness verification failed")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
