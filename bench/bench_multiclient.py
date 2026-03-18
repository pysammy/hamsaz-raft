#!/usr/bin/env python3
"""
True multi-client benchmark driver:
- Spawns N raft_node_server processes
- Uses real TCP clients (multiple in-flight requests) against API ports
- Records latency + wall throughput and parses Raft/gate counters from logs
"""

import argparse
import csv
import json
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple


def now_ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def check_port(host: str, port: int) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.close()
        return True
    except Exception:
        return False


class ProcLog:
    def __init__(self, proc: subprocess.Popen):
        self.proc = proc
        self.buf: List[str] = []
        self.mu = threading.Lock()
        self.t = threading.Thread(target=self._reader, daemon=True)
        self.t.start()

    def _reader(self):
        while True:
            line = self.proc.stdout.readline()
            if not line:
                break
            with self.mu:
                self.buf.append(line)

    def lines(self) -> List[str]:
        with self.mu:
            return list(self.buf)


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


def parse_members(resp: str) -> int:
    parts = resp.split("\t")
    if len(parts) == 2 and parts[0] == "MEMBERS":
        return int(parts[1])
    return -1


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


def parse_stats(resp: str) -> Tuple[int, int, int, int]:
    # STATS \t raft_appends \t queued \t appended_from_queue \t dropped
    parts = resp.split("\t")
    if len(parts) != 5 or parts[0] != "STATS":
        raise RuntimeError(f"bad STATS response: {resp}")
    return int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])


def build_workload(ops: int, conflict_ratio: float, dependent_ratio: float) -> List[str]:
    num_conflict = int(ops * conflict_ratio)
    num_dependent = int(ops * dependent_ratio)
    num_independent = ops - (num_conflict * 2 + num_dependent)
    if num_independent < 0:
        num_independent = 0

    out: List[str] = []
    # 1) deletes first, so gate can queue some of them.
    for i in range(num_conflict):
        out.append(f"delete\tcourseC{i}\t_")
    # 2) add prerequisites after deletes.
    for i in range(num_conflict):
        out.append(f"add\tcourseC{i}\t_")
    # 3) dependents.
    base = num_conflict if num_conflict > 0 else 1
    for i in range(num_dependent):
        out.append(f"enroll\tstudent{i}\tcourseC{i % base}")
    # 4) independent adds.
    for i in range(num_independent):
        out.append(f"add\tcourseI{i}\t_")
    return out


def launch_servers(args, ports: List[int], api_ports: List[int]):
    procs = []
    logs = []
    for i in range(args.nodes):
        node_id = i + 1
        cmd = [
            str(Path(args.build) / "raft_node_server"),
            "--id", str(node_id),
            "--port", str(ports[i]),
            "--api-port", str(api_ports[i]),
            "--gossip-delay", args.gossip_delay,
            "--gossip-drop", str(args.gossip_drop),
        ]
        for j in range(args.nodes):
            if j == i:
                continue
            cmd.extend(["--peer", f"{j+1}:127.0.0.1:{ports[j]}"])
        if args.inproc:
            cmd.append("--inproc")
        else:
            if i != 0:
                cmd.append("--join")
        if args.gossip_udp:
            cmd.extend(["--gossip-udp", str(ports[i] + 1000)])
        if args.all_to_raft:
            cmd.append("--all-to-raft")

        p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, bufsize=1)
        procs.append(p)
        logs.append(ProcLog(p))
        time.sleep(0.4)
    return procs, logs


def wait_cluster_ready(api_ports: List[int], expected_members: int, timeout_sec: float = 30.0):
    c = NodeConn("127.0.0.1", api_ports[0], timeout=2.0)
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


def run_workload(api_ports: List[int], ops: List[str], concurrency: int, op_prefix: str,
                 max_retries: int, retry_delay_ms: int, fail_on_op_errors: bool,
                 request_timeout_sec: float):
    thread_local = threading.local()
    latencies: List[float] = []
    lat_mu = threading.Lock()
    fail_mu = threading.Lock()
    all_conns: List[NodeConn] = []
    conn_mu = threading.Lock()
    failures = 0

    first_send = None
    last_finish = None
    time_mu = threading.Lock()

    targets = [random.randrange(len(api_ports)) for _ in ops]

    def get_conn(node_idx: int) -> NodeConn:
        conn_map: Dict[int, NodeConn] = getattr(thread_local, "conn_map", None)
        if conn_map is None:
            conn_map = {}
            thread_local.conn_map = conn_map
        c = conn_map.get(node_idx)
        if c is None:
            c = NodeConn("127.0.0.1", api_ports[node_idx], timeout=request_timeout_sec)
            conn_map[node_idx] = c
            with conn_mu:
                all_conns.append(c)
        return c

    def one(i: int):
        nonlocal first_send, last_finish, failures
        node_idx = targets[i]
        cmd, a1, a2 = ops[i].split("\t")
        op_id = f"{op_prefix}-op-{i+1}"
        line = f"EXEC\t{op_id}\t{cmd}\t{a1}\t{a2}"
        c = get_conn(node_idx)
        max_attempts = max(1, max_retries + 1)
        for attempt in range(max_attempts):
            t0 = time.time()
            with time_mu:
                if first_send is None:
                    first_send = t0
            resp = c.request(line)
            t1 = time.time()
            with time_mu:
                last_finish = t1
            with lat_mu:
                latencies.append(t1 - t0)

            parts = resp.split("\t", 2)
            if len(parts) < 2 or parts[0] != "RES":
                raise RuntimeError(f"bad response: {resp}")
            ok = parts[1] == "1"
            msg = parts[2] if len(parts) >= 3 else ""
            # Deferred means accepted for eventual processing.
            if ok or msg.startswith("Deferred"):
                return
            if attempt + 1 >= max_attempts:
                if fail_on_op_errors:
                    raise RuntimeError(f"operation failed after retries: {resp}")
                with fail_mu:
                    failures += 1
                return
            # Followers in older builds may reject conflicting writes.
            # Retry by re-targeting another node (likely leader) instead of pinning one endpoint.
            lower_msg = msg.lower()
            if "raft append rejected" in lower_msg or "not leader" in lower_msg:
                node_idx = random.randrange(len(api_ports))
                c = get_conn(node_idx)
            time.sleep(max(0, retry_delay_ms) / 1000.0)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        list(pool.map(one, range(len(ops))))

    wall = 0.0 if first_send is None or last_finish is None else (last_finish - first_send)

    for c in all_conns:
        c.close()
    return latencies, wall, failures


def parse_server_stats(logs: List[ProcLog]) -> Tuple[List[int], Dict[str, int]]:
    raft_appends: List[int] = []
    gate_stats = {"queued": 0, "appended_from_queue": 0, "dropped": 0}

    for log in logs:
        for line in log.lines():
            if "total raft appends:" in line:
                parts = line.strip().split()
                raft_appends.append(int(parts[-1]))
            if "conflict gate queued:" in line:
                parts = line.strip().split()
                gate_stats["queued"] += int(parts[-5])
                gate_stats["appended_from_queue"] += int(parts[-3])
                gate_stats["dropped"] += int(parts[-1])

    return raft_appends, gate_stats


def verify_correctness(api_ports: List[int],
                       settle_timeout_sec: float,
                       settle_poll_ms: int):
    t0 = time.time()
    attempts = 0
    last_state = None
    while True:
        attempts += 1
        nodes = []
        for i, port in enumerate(api_ports, start=1):
            c = NodeConn("127.0.0.1", port, timeout=2.0)
            try:
                invariant_ok = parse_invariant(c.request("INVARIANT"))
                state = parse_state_hash(c.request("STATE_HASH"))
                nodes.append({
                    "node": i,
                    "endpoint": f"127.0.0.1:{port}",
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


def fetch_gate_stats_api(api_ports: List[int]) -> Dict[str, int]:
    gate = {"queued": 0, "appended_from_queue": 0, "dropped": 0}
    for port in api_ports:
        c = NodeConn("127.0.0.1", port, timeout=2.0)
        try:
            _, q, aq, dr = parse_stats(c.request("STATS"))
            gate["queued"] += q
            gate["appended_from_queue"] += aq
            gate["dropped"] += dr
        finally:
            c.close()
    return gate


def pending_conflicts(gate: Dict[str, int]) -> int:
    return max(0, gate["queued"] - gate["appended_from_queue"] - gate["dropped"])


def drain_conflict_queue(api_ports: List[int], timeout_sec: float, poll_ms: int):
    t0 = time.time()
    attempts = 0
    first_gate = fetch_gate_stats_api(api_ports)
    while True:
        attempts += 1
        gate = fetch_gate_stats_api(api_ports)
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


def request_shutdown(api_port: int):
    c = NodeConn("127.0.0.1", api_port, timeout=2.0)
    try:
        c.request("SHUTDOWN")
    except Exception:
        pass
    finally:
        c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default="build")
    ap.add_argument("-n", "--nodes", type=int, default=10)
    ap.add_argument("--base-port", type=int, default=15000, help="Raft base port")
    ap.add_argument("--api-base-port", type=int, default=25000, help="TCP API base port")
    ap.add_argument("--ops", type=int, default=1500)
    ap.add_argument("--conflict-ratio", type=float, default=0.25)
    ap.add_argument("--dependent-ratio", type=float, default=0.25)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--gossip-udp", action="store_true")
    ap.add_argument("--gossip-sim-only", action="store_true",
                    help="do not auto-enable UDP gossip for split-routing runs")
    ap.add_argument("--gossip-delay", default="2-8")
    ap.add_argument("--gossip-drop", type=float, default=0.0)
    ap.add_argument("--all-to-raft", action="store_true")
    ap.add_argument("--no-conflict-gate", action="store_true",
                    help="deprecated/no-op; conflict gate is always enabled")
    ap.add_argument("--inproc", action="store_true")
    ap.add_argument("--out", default="artifacts/bench_multiclient")
    ap.add_argument("--no-port-probe", action="store_true")
    ap.add_argument("--verify-correctness", action="store_true",
                    help="verify convergence (state hash + invariants) after workload")
    ap.add_argument("--settle-timeout-sec", type=float, default=8.0,
                    help="max time to wait for convergence checks")
    ap.add_argument("--settle-poll-ms", type=int, default=200,
                    help="poll interval for convergence checks")
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
    ap.add_argument("--drain-timeout-sec", type=float, default=3.0,
                    help="max time to wait for queued conflict ops to drain before correctness check")
    ap.add_argument("--drain-poll-ms", type=int, default=200,
                    help="poll interval for queue-drain checks")
    args = ap.parse_args()
    if args.no_conflict_gate:
        print("[bench_mc] --no-conflict-gate is ignored; conflict gate is always enabled")
    if not args.all_to_raft and not args.gossip_udp and not args.gossip_sim_only:
        args.gossip_udp = True
        print("[bench_mc] auto-enabled --gossip-udp for split-routing across processes")

    run_dir = Path(args.out) / now_ts()
    run_dir.mkdir(parents=True, exist_ok=True)

    for path in (Path("data"), Path("logs")):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    for p in Path(".").glob("gossip_state_*.bin"):
        try:
            p.unlink()
        except OSError:
            pass

    raft_ports = [args.base_port + i for i in range(args.nodes)]
    api_ports = [args.api_base_port + i for i in range(args.nodes)]

    if not args.no_port_probe:
        for p in raft_ports + api_ports:
            if not check_port("127.0.0.1", p):
                print(f"[bench_mc] ERROR: cannot bind 127.0.0.1:{p}")
                return 1

    procs, logs = launch_servers(args, raft_ports, api_ports)
    print(f"[bench_mc] launched {len(procs)} servers")

    try:
        wait_cluster_ready(api_ports, args.nodes)
        print("[bench_mc] cluster ready")

        ops = build_workload(args.ops, args.conflict_ratio, args.dependent_ratio)
        print(f"[bench_mc] sending {len(ops)} ops concurrency={args.concurrency}")
        op_prefix = f"mc-{now_ts()}-{random.randrange(1_000_000)}"
        latencies, wall_span, op_failures = run_workload(
            api_ports, ops, args.concurrency, op_prefix,
            args.max_retries, args.retry_delay_ms, args.fail_on_op_errors,
            args.request_timeout_sec
        )
        correctness = None
        drain_info = None
        if args.verify_correctness:
            drain_info = drain_conflict_queue(api_ports, args.drain_timeout_sec, args.drain_poll_ms)
            correctness = verify_correctness(api_ports, args.settle_timeout_sec, args.settle_poll_ms)

        for p in api_ports:
            request_shutdown(p)

        for p in procs:
            try:
                p.wait(timeout=6)
            except subprocess.TimeoutExpired:
                p.terminate()

        for i, log in enumerate(logs, start=1):
            with open(run_dir / f"node{i}.log", "w") as f:
                f.writelines(log.lines())

        raft_appends, gate_stats = parse_server_stats(logs)

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
            "nodes": args.nodes,
            "ops": total,
            "conflict_ratio": args.conflict_ratio,
            "dependent_ratio": args.dependent_ratio,
            "concurrency": args.concurrency,
            "all_to_raft": args.all_to_raft,
            "conflict_gate": True,
            "avg_latency_sec": avg,
            "throughput_ops_per_sec": rtt_throughput,
            "wall_clock_span_sec": wall_span,
            "wall_throughput_ops_per_sec": wall_throughput,
            "raft_appends": raft_appends,
            "conflict_gate_stats": gate_stats,
            "drain": drain_info,
            "correctness": correctness,
            "op_failures": op_failures,
        }
        with open(run_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        print(f"[bench_mc] avg latency {avg:.6f}s throughput_rtt {rtt_throughput:.1f} wall_throughput {wall_throughput:.1f}")
        print(f"[bench_mc] op_failures={op_failures}")
        if drain_info:
            print(f"[bench_mc] drain drained={drain_info['drained']} pending={drain_info['pending']} "
                  f"settle={drain_info['settle_time_sec']:.3f}s")
        if correctness:
            print(f"[bench_mc] correctness converged={correctness['converged']} "
                  f"invariants={correctness['all_invariants_ok']} "
                  f"settle={correctness['settle_time_sec']:.3f}s")
        print(f"[bench_mc] artifacts: {run_dir}")

        try:
            import matplotlib.pyplot as plt
            xs = list(range(total))
            plt.figure()
            plt.plot(xs, latencies, marker=".", linestyle="none", alpha=0.4)
            plt.xlabel("op index")
            plt.ylabel("latency (s)")
            plt.title("Per-op latency (true multi-client)")
            plt.savefig(run_dir / "latency_scatter.png", dpi=150)
            plt.close()

            plt.figure()
            sl = sorted(latencies)
            ys = [i / total for i in range(total)]
            plt.plot(sl, ys)
            plt.xlabel("latency (s)")
            plt.ylabel("CDF")
            plt.title("Latency CDF (true multi-client)")
            plt.grid(True)
            plt.savefig(run_dir / "latency_cdf.png", dpi=150)
            plt.close()
            print("[bench_mc] plots written")
        except Exception as e:
            print(f"[bench_mc] plotting skipped ({e})")

        if args.fail_on_correctness and correctness and not correctness["converged"]:
            print("[bench_mc] correctness verification failed")
            return 2
        return 0
    finally:
        for p in api_ports:
            try:
                request_shutdown(p)
            except Exception:
                pass
        for p in procs:
            if p.poll() is None:
                p.terminate()


if __name__ == "__main__":
    sys.exit(main())
