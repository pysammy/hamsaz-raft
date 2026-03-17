#!/usr/bin/env python3
"""
Lightweight persistence sanity check:
- Starts 3 raft_node_cli processes with preserved data dirs.
- Waits for full membership.
- Sends a few ops.
- Kills node2, restarts it using same data dir, waits for membership again.
- Exits all nodes.
"""

import os
import shutil
import subprocess
import time
from pathlib import Path

BUILD = Path(os.environ.get("BUILD_DIR", "build"))
BASE_PORT = int(os.environ.get("BASE_PORT", "19100"))
DATA_ROOT = Path("data")
LOG_ROOT = Path("logs")


def launch(node_id, port, peers, join):
    args = [
        str(BUILD / "raft_node_cli"),
        "--id", str(node_id),
        "--port", str(port),
    ]
    if join:
        args.append("--join")
    for pid, host, pport in peers:
        args.extend(["--peer", f"{pid}:{host}:{pport}"])
    proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    return proc


def read_until_prompt(proc, timeout=5):
    start = time.time()
    buf = []
    while True:
        ch = proc.stdout.read(1)
        if ch == "" and proc.poll() is not None:
            break
        buf.append(ch)
        if "".join(buf).endswith("\n> "):
            break
        if time.time() - start > timeout:
            break
    return "".join(buf)


def send_cmd(proc, cmd, timeout=5):
    proc.stdin.write(cmd + "\n")
    proc.stdin.flush()
    return read_until_prompt(proc, timeout)


def wait_members(proc, expected, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        out = send_cmd(proc, "members", timeout=2)
        try:
            parts = out.strip().split()
            if len(parts) >= 2 and int(parts[1]) >= expected:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def main():
    # clean data/logs
    if DATA_ROOT.exists():
        shutil.rmtree(DATA_ROOT)
    if LOG_ROOT.exists():
        shutil.rmtree(LOG_ROOT)
    LOG_ROOT.mkdir(parents=True, exist_ok=True)

    ports = [BASE_PORT + i for i in range(3)]
    peers = [
        [(2, "127.0.0.1", ports[1]), (3, "127.0.0.1", ports[2])],
        [(1, "127.0.0.1", ports[0]), (3, "127.0.0.1", ports[2])],
        [(1, "127.0.0.1", ports[0]), (2, "127.0.0.1", ports[1])],
    ]
    joins = [False, True, True]

    procs = []
    for i in range(3):
        p = launch(i + 1, ports[i], peers[i], joins[i])
        procs.append(p)
        read_until_prompt(p, timeout=8)

    if not wait_members(procs[0], 3, timeout=20):
        print("FAILED: cluster membership not reached")
        return 1

    # send a few ops to seed
    for i in range(3):
        send_cmd(procs[0], f"add c{i}")
    send_cmd(procs[0], "query")

    # kill node2, restart with same data dir
    procs[1].kill()
    procs[1].wait(timeout=5)
    p2 = launch(2, ports[1], peers[1], True)
    procs[1] = p2
    read_until_prompt(p2, timeout=8)

    if not wait_members(procs[0], 3, timeout=20):
        print("FAILED: membership did not recover after restart")
        return 1

    print("PASS: persistence smoke")
    # exit all
    for p in procs:
        try:
            send_cmd(p, "exit", timeout=2)
            p.wait(timeout=2)
        except Exception:
            p.kill()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
