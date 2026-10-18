#!/usr/bin/env python3
"""Median and spread of every metric per server, from a CSV made by run_benchmark.sh.

    python3 scripts/summarize_results.py benchmark_results/results_20261015_120000.csv
"""
import csv
import statistics
import sys

METRICS = [
    ("throughput_msgs_per_s", "throughput (replies/s)"),
    ("p50_us", "latency p50 (us)"),
    ("p99_us", "latency p99 (us)"),
    ("p999_us", "latency p99.9 (us)"),
    ("max_us", "latency max (us)"),
    ("lost", "lost / unanswered"),
]


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)

    runs_by_server = {}
    with open(sys.argv[1], newline="") as results_file:
        for row in csv.DictReader(results_file):
            if row["throughput_msgs_per_s"] == "":  # a run that failed has no numbers
                print("note: run %s of %s produced no result, skipped" % (row["run"], row["server"]))
                continue
            runs_by_server.setdefault(row["server"], []).append(row)

    for server, rows in runs_by_server.items():
        print("\n%s  (%d runs)" % (server, len(rows)))
        print("  %-26s %14s %14s %14s" % ("", "median", "min", "max"))
        for column, label in METRICS:
            values = [float(row[column]) for row in rows]
            print("  %-26s %14.1f %14.1f %14.1f" % (label, statistics.median(values), min(values), max(values)))


if __name__ == "__main__":
    main()
