#!/usr/bin/env python3
"""
cpu_profiler.py
---------------
Record container CPU utilisation (0–100 % of its quota) to a CSV file
and echo each sample to stdout in real time.

Usage:
    python cpu_profiler.py --file cpu_usage.csv --interval 0.5
"""

import argparse
import csv
import datetime as dt
from pathlib import Path
import time

import psutil


# ───── cgroup helpers ───────────────────────────────────────────────────
def get_cpu_quota_vcpus() -> float | None:
    """Return the pod’s CPU quota in vCPUs, or None if unlimited."""
    cpu_max = Path("/sys/fs/cgroup/cpu.max")
    if cpu_max.exists():
        quota, period = cpu_max.read_text().strip().split()
        if quota != "max" and period != "max":
            try:
                return int(quota) / int(period)
            except (ValueError, ZeroDivisionError):
                pass
    return None


def normalised_cpu_percent(raw_pct: float, quota_vcpus: float | None) -> float:
    """Scale host-level cpu_percent to 0–100 % of the quota and cap it."""
    if quota_vcpus and quota_vcpus > 0:
        host_vcpus = psutil.cpu_count(logical=True)
        scaled = raw_pct * host_vcpus / quota_vcpus
    else:
        scaled = raw_pct
    return max(0.0, min(100.0, scaled))


# ───── CSV helper ───────────────────────────────────────────────────────
def ensure_header(path: Path, header=("timestamp", "cpu_percent")) -> None:
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(header)


# ───── main loop ────────────────────────────────────────────────────────
def main(file_path: str, interval: float) -> None:
    out = Path(file_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    ensure_header(out)

    quota_vcpus = get_cpu_quota_vcpus()
    psutil.cpu_percent(interval=None)  # prime counters

    print(f"Recording CPU every {interval}s → {out.resolve()}")
    if quota_vcpus:
        print(f"Detected CPU quota ≈ {quota_vcpus:.2f} vCPUs (capped at 100 %).")

    while True:
        raw = psutil.cpu_percent(interval=None)
        pct = normalised_cpu_percent(raw, quota_vcpus)
        ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

        # write to CSV
        with out.open("a", newline="") as f:
            csv.writer(f).writerow((ts, f"{pct:.2f}"))

        # live console output
        print(f"{ts},{pct:.2f}", flush=True)

        time.sleep(interval)


# ───── CLI ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple container CPU profiler")
    parser.add_argument("-f", "--file", default="cpu_usage.csv",
                        help="CSV output file (default: cpu_usage.csv)")
    parser.add_argument("-i", "--interval", type=float, default=0.5,
                        help="Sampling interval in seconds (default: 0.5)")
    args = parser.parse_args()

    try:
        main(args.file, args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")