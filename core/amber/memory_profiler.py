#!/usr/bin/env python3
"""
memory_profiler.py
------------------
Record container memory utilisation (0–100 % of its limit) to a CSV file
and echo each sample to stdout in real time.

Examples
--------
    # log every sample
    python memory_profiler.py --file memory_usage.csv --interval 0.5

    # log only when a new peak is reached
    python memory_profiler.py --file memory_peak.csv --interval 0.5 --max
"""
import argparse
import csv
import datetime as dt
from pathlib import Path
import time
import psutil

# ───── cgroup helpers ───────────────────────────────────────────────────
CGROUP_V2 = Path("/sys/fs/cgroup/memory.max").exists()


def _read_first(path: Path) -> str | None:
    try:
        return path.read_text().strip().split()[0]
    except FileNotFoundError:
        return None


def get_mem_limit_bytes() -> int | None:
    if CGROUP_V2:
        raw = _read_first(Path("/sys/fs/cgroup/memory.max"))
    else:
        raw = _read_first(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
    if raw is None or raw == "max":
        return None
    try:
        val = int(raw)
        if val >= 1 << 60:  # ~1 EiB => treat as unlimited
            return None
        return val
    except ValueError:
        return None


def get_mem_usage_bytes() -> int:
    path = Path("/sys/fs/cgroup/memory.current" if CGROUP_V2
                else "/sys/fs/cgroup/memory/memory.usage_in_bytes")
    raw = _read_first(path)
    return int(raw) if raw and raw.isdigit() else 0


def normalised_mem_percent(usage_b: int, limit_b: int | None) -> float:
    if limit_b:
        pct = usage_b / limit_b * 100
    else:
        pct = usage_b / psutil.virtual_memory().total * 100
    return max(0.0, min(100.0, pct))


# ───── CSV helpers ──────────────────────────────────────────────────────
def ensure_header(path: Path,
                  header=("timestamp", "memory_percent")) -> None:
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(header)


def last_recorded_pct(path: Path) -> float | None:
    """Return the last recorded memory_percent or None if none exists."""
    if not path.exists():
        return None
    with path.open("rb") as f:
        try:
            f.seek(-2, 2)          # jump to just before file end
            while f.read(1) != b"\n":
                f.seek(-2, 1)      # scan backwards to find newline
        except OSError:            # 1‑line file
            f.seek(0)
        last_line = f.readline().decode(errors="ignore").strip()
    try:
        return float(last_line.split(",")[-1])
    except (IndexError, ValueError):
        return None


# ───── main loop ────────────────────────────────────────────────────────
def main(file_path: str, interval: float, only_max: bool) -> None:
    out = Path(file_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    ensure_header(out)
    limit_b = get_mem_limit_bytes()

    if limit_b:
        print(f"Detected memory limit ≈ {limit_b / (1024**2):.1f} MiB "
              "(values capped at 100 %).")
    else:
        print("No memory limit detected; percentage is of node total.")

    mode = "PEAK‑ONLY" if only_max else "EVERY SAMPLE"
    print(f"[{mode}] Recording memory every {interval}s → {out.resolve()}")

    while True:
        usage_b = get_mem_usage_bytes()
        pct = normalised_mem_percent(usage_b, limit_b)
        ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

        should_write = True
        if only_max:
            last_pct = last_recorded_pct(out)
            should_write = last_pct is None or pct > last_pct

        if should_write:
            with out.open("a", newline="") as f:
                csv.writer(f).writerow((ts, f"{pct:.2f}"))

        # live console output
        print(f"{ts},{pct:.2f}", flush=True)
        time.sleep(interval)


# ───── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simple container memory profiler")
    parser.add_argument("-f", "--file", default="memory_usage.csv",
                        help="CSV output file (default: memory_usage.csv)")
    parser.add_argument("-i", "--interval", type=float, default=0.5,
                        help="Sampling interval in seconds (default: 0.5)")
    parser.add_argument("-m", "--max", action="store_true",
                        help="Only record a line when a new peak "
                             "memory‑usage percentage is reached")

    args = parser.parse_args()
    try:
        main(args.file, args.interval, args.max)
    except KeyboardInterrupt:
        print("\nStopped.")