#!/usr/bin/env python3
"""Latency benchmark driver (SIS #2, Notification Service Ingest API).

For every (protocol, payload, rps) point and every repetition:
  1. start a FRESH server JVM (pinned to CPU 0 when `taskset` is available)  -> no state leaks between runs
  2. start the load generator JVM (pinned to CPU 1 when `taskset`)           -> client and server do not fight
  3. the generator warms up (discarded), then measures                       -> open-loop fixed rate, HdrHistogram
  4. kill the server, append one CSV row (with the run number)
Final tables use the MEDIAN over runs for each percentile.

Note on macOS: `taskset` is not available; the OS scheduler shares cores between server and client.
This is documented in the README; cross-platform cross-run numbers are not directly comparable.

usage: run_bench.py <out.csv> <suite> [--runs 3] [--duration 10] [--warmup 5]
suites: sizes | rps | ramp | smoke
"""
import argparse, csv, io, os, re, subprocess, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_cpfile = os.path.join(ROOT, "build/classpath.txt")
# offline build (build-offline.sh) writes build/classpath.txt; with Maven use target/classes + target/lib/*
CP = ("build/classes:" + open(_cpfile).read().strip()) if os.path.exists(_cpfile) else "target/classes:target/lib/*"
HAVE_TASKSET = subprocess.call(["which", "taskset"], stdout=subprocess.DEVNULL) == 0 and os.cpu_count() >= 2


def pin(cpu):
    return ["taskset", "-c", str(cpu)] if HAVE_TASKSET else []


def free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def client_threads(rps):
    """At high rps the open-loop generator needs more worker threads so submitted calls do not back up."""
    if rps >= 15000:
        return 512
    if rps >= 6000:
        return 256
    return 128


def run_once(proto, mode, size, rps, duration, warmup, run_no):
    base = proto.split("-")[0]                      # grpc-gzip -> grpc (no gzip in latency suites, kept for future)
    port = free_port()
    server = subprocess.Popen(
        pin(0) + ["java", "-Xms512m", "-Xmx512m", "-XX:+UseG1GC", "-cp", CP, "sis2.app.ServerMain",
                  "--proto", base, "--port", str(port), "--threads", "16"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        for line in server.stdout:
            if line.startswith("READY"):
                break
        out = subprocess.run(
            pin(1) + ["java", "-Xms256m", "-Xmx768m", "-XX:+UseG1GC", "-cp", CP, "sis2.bench.LoadGenerator",
                      "--proto", proto, "--port", str(port), "--rps", str(rps), "--duration", str(duration),
                      "--warmup", str(warmup), "--size", str(size), "--threads", str(client_threads(rps))],
            cwd=ROOT, capture_output=True, text=True, timeout=(duration + warmup) * 6 + 120)
        rows = [l for l in out.stdout.splitlines() if l and not l.startswith("proto,")]
        if not rows:
            raise RuntimeError("no result: " + out.stderr[-500:])
        return f"{run_no}," + rows[-1]
    finally:
        server.terminate()
        try:
            server.wait(5)
        except subprocess.TimeoutExpired:
            server.kill()


def suite(name):
    """SIS #1 workload: 3000 events/s steady, 10000 events/s peak; avg 1 KB, max 10 KB per event."""
    pts = []
    if name == "smoke":
        pts = [("grpc", "text", 1024, 1000), ("rest", "text", 1024, 1000)]
    if name == "sizes":     # 3 SIS #1 payload sizes, all at the TARGET rate (3000 rps)
        for size in (100, 1024, 10240):
            for proto in ("rest", "grpc"):
                pts.append((proto, "text", size, 3000))
    if name == "rps":       # SIS #1 load curve at 1 KB (average payload): 1500, 3000 target, 6000, 10000 peak
        for rps in (1500, 3000, 6000, 10000):
            for proto in ("rest", "grpc"):
                pts.append((proto, "text", 1024, rps))
    if name == "ramp":      # capacity probe above SIS #1 peak: find where each protocol saturates (keep all points)
        for rps in (15000, 20000, 30000, 40000):
            for proto in ("rest", "grpc"):
                pts.append((proto, "text", 1024, rps))
    return pts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out"); ap.add_argument("suite")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--duration", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=5)
    a = ap.parse_args()

    header = "run,proto,mode,size,rps_target,rps_achieved,ok,errors,p50_us,p90_us,p95_us,p99_us,p999_us,max_us,svc_p50_us,svc_p99_us"
    new = not os.path.exists(a.out)
    with open(a.out, "a") as f:
        if new:
            f.write(header + "\n")
        pts = suite(a.suite)
        total = len(pts) * a.runs
        n = 0
        for run_no in range(1, a.runs + 1):
            for proto, mode, size, rps in pts:   # interleave protocols inside a run => drift hits both equally
                n += 1
                t = time.time()
                row = run_once(proto, mode, size, rps, a.duration, a.warmup, run_no)
                f.write(row + "\n"); f.flush()
                print(f"[{n}/{total}] {proto:5} {mode} {size:>6} @ {rps:>5} rps run {run_no}: {row.split(',')[8:12]} "
                      f"({time.time()-t:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
