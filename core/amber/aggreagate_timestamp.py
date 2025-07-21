#!/usr/bin/env python3
"""
aggregate_timestamps.py
-----------------------
Scan every file whose name begins with “time‑stamp”, find the earliest
START timestamp and the latest END timestamp, and print the total
elapsed time in seconds.

Each timestamp is expected to be in nanoseconds, written exactly as:
    START 1699999999999999999
    END   1700000000000000000
"""

from pathlib import Path
from typing import Optional, Tuple

LOG_GLOB = "time-stamp*.log"           # pattern for the log files


def find_extremes() -> Tuple[Optional[int], Optional[int]]:
    """Return (earliest_start_ns, latest_end_ns); any value may be None."""
    earliest: Optional[int] = None
    latest:   Optional[int] = None

    for log_path in Path(".").glob(LOG_GLOB):
        with log_path.open() as f:
            for line in f:
                parts = line.strip().split(maxsplit=1)
                if len(parts) != 2:
                    continue
                tag, ns_str = parts
                try:
                    ts = int(ns_str)
                except ValueError:
                    continue

                if tag == "START":
                    if earliest is None or ts < earliest:
                        earliest = ts
                elif tag == "END":
                    if latest is None or ts > latest:
                        latest = ts

    return earliest, latest


def main() -> None:
    earliest, latest = find_extremes()

    if earliest is None:
        print("❗ No START timestamps found.")
        return
    if latest is None:
        print("❗ No END timestamps found.")
        return

    elapsed_seconds = (latest - earliest) / 1_000_000_000
    print(f"Earliest START : {earliest}")
    print(f"Latest   END   : {latest}")
    print(f"Total elapsed  : {elapsed_seconds:.6f} s")


if __name__ == "__main__":
    main()
