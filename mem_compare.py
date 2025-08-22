#!/usr/bin/env python3

"""
mem_compare.py — compare memory usage across multiple JSON logs (duckdb/sqlite/postgres)

Each input JSON is expected to look like:
{
  "config": {...},
  "stats": [
    {"timestamp": ..., "iteration": 0, "rss": 108318720, "heapTotal": ..., ...},
    {"timestamp": ..., "iteration": 1, "rss": 111071232, ...},
    ...
  ]
}

Features:
- Finds the max common iteration across all files (i.e., min of per-file max iteration).
- Trims series to that common range for apples-to-apples comparison.
- Downsamples to a target number of points by averaging within buckets.
- Plots a PNG (matplotlib) and/or writes a reduced JSON for the web.
- Choose the metric (rss, heapUsed, heapTotal, external, arrayBuffers). Default: rss.

Usage examples:
  # Quick plot (RSS in MB) of the three files, ~300 points, output plot.png
  ./mem_compare.py memory_stats_*.json --output-plot plot.png --target-points 300

  # Create a smaller JSON suitable for a web page (Plotly/D3/etc)
  ./mem_compare.py memory_stats_*.json --output-json reduced.json --target-points 400

  # Change metric to heapUsed
  ./mem_compare.py memory_stats_*.json --metric heapUsed --output-plot heap_used.png
"""
import argparse
import json
import math
import os
from typing import List, Dict, Any, Tuple, Optional

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt


Metric = str


def human_metric_name(metric: Metric) -> str:
    mapping = {
        "rss": "RSS",
        "heapUsed": "Heap Used",
        "heapTotal": "Heap Total",
        "external": "External",
        "arrayBuffers": "Array Buffers",
    }
    return mapping.get(metric, metric)


def bytes_like_to_mb(value: float, metric: Metric) -> float:
    """
    Convert value to MB if it's a byte-like metric. For arrayBuffers we also treat as bytes.
    """
    if metric in ("rss", "heapUsed", "heapTotal", "external", "arrayBuffers"):
        return float(value) / (1024 * 1024.0)
    return float(value)


def load_series(path: str, metric: Metric) -> Tuple[str, List[int], List[float]]:
    """
    Returns (label, iterations, values_mb)
    Label is derived from filename, e.g., postgres/sqlite/duckdb inferred from path.
    """
    with open(path, "r") as f:
        data = json.load(f)

    stats = data.get("stats", [])
    iterations = []
    values = []
    for row in stats:
        it = row.get("iteration")
        if it is None:
            continue
        val = row.get(metric)
        if val is None:
            # If metric is missing in a row, skip it.
            continue
        iterations.append(int(it))
        values.append(bytes_like_to_mb(val, metric))

    # Guess a nicer label from filename:
    base = os.path.basename(path).lower()
    if "duckdb" in base:
        label = "DuckDB"
    elif "sqlite" in base:
        label = "SQLite"
    elif "postgres" in base or "postgre" in base:
        label = "Postgres"
    else:
        label = os.path.basename(path)
    return label, iterations, values


def align_to_common_iteration(
    series: List[Tuple[str, List[int], List[float]]]
) -> Tuple[int, List[Tuple[str, List[int], List[float]]]]:
    """
    Trims each series to [0..max_common_iter], where max_common_iter is the minimum of the
    maximum iteration discovered in each input.
    """
    max_iters = []
    for _, iters, _ in series:
        if not iters:
            max_iters.append(-1)
        else:
            max_iters.append(max(iters))
    max_common = min(max_iters) if max_iters else -1

    aligned = []
    for label, iters, vals in series:
        trimmed_iters = []
        trimmed_vals = []
        for i, v in zip(iters, vals):
            if 0 <= i <= max_common:
                trimmed_iters.append(i)
                trimmed_vals.append(v)
        aligned.append((label, trimmed_iters, trimmed_vals))
    return max_common, aligned


def bucket_downsample(
    iters: List[int],
    vals: List[float],
    target_points: int,
) -> Tuple[List[int], List[float]]:
    """
    Reduce to ~target_points by bucketing across iteration range and averaging.
    Keeps the first and last points exactly (if present).
    """
    if not iters or len(iters) <= target_points:
        return iters, vals

    min_it = min(iters)
    max_it = max(iters)
    if max_it == min_it:
        # Degenerate case: all iterations the same
        return [iters[0]], [sum(vals) / len(vals)]

    # We create evenly spaced bucket edges across [min_it, max_it]
    buckets = target_points
    edges = [min_it + (x * (max_it - min_it) / buckets) for x in range(buckets + 1)]

    # Accumulate values per bucket
    bucket_sums = [0.0] * buckets
    bucket_counts = [0] * buckets
    for i, v in zip(iters, vals):
        if i == max_it:
            idx = buckets - 1
        else:
            # find bucket index
            idx = int((i - min_it) * buckets / (max_it - min_it))
            idx = min(max(idx, 0), buckets - 1)
        bucket_sums[idx] += v
        bucket_counts[idx] += 1

    down_iters = []
    down_vals = []
    for b in range(buckets):
        if bucket_counts[b] > 0:
            # represent the bucket by its center iteration and average value
            left = edges[b]
            right = edges[b + 1]
            center = int(round((left + right) / 2.0))
            down_iters.append(center)
            down_vals.append(bucket_sums[b] / bucket_counts[b])

    # Ensure first/last points correspond to min/max iteration if possible
    if down_iters:
        down_iters[0] = min_it
        down_iters[-1] = max_it

    return down_iters, down_vals


def write_reduced_json(
    out_path: str,
    metric: Metric,
    series: List[Tuple[str, List[int], List[float]]],
) -> None:
    """
    Writes a compact JSON structure for the web:
    {
      "metric": "rss",
      "unit": "MB",
      "series": [
        {"label": "DuckDB", "points": [{"iteration": 0, "value": 103.2}, ...]},
        {"label": "Postgres", "points": [...]}
      ]
    }
    """
    out = {
        "metric": metric,
        "unit": "MB",
        "series": []
    }
    for label, iters, vals in series:
        points = [{"iteration": int(i), "value": float(v)} for i, v in zip(iters, vals)]
        out["series"].append({"label": label, "points": points})
    with open(out_path, "w") as f:
        json.dump(out, f, separators=(",", ":"))  # minified


def plot_series(
    metric: Metric,
    series: List[Tuple[str, List[int], List[float]]],
    out_path: str,
) -> None:
    plt.figure(figsize=(10, 6))
    for label, iters, vals in series:
        if iters and vals:
            plt.plot(iters, vals, label=label)
    plt.xlabel("Iteration")
    plt.ylabel(f"{human_metric_name(metric)} (MB)")
    plt.title(f"{human_metric_name(metric)} vs Iteration (aligned to common max iteration)")
    plt.legend()
    plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)


def main():
    parser = argparse.ArgumentParser(description="Compare memory usage across JSON logs.")
    parser.add_argument("files", nargs="+", help="Input JSON files")
    parser.add_argument("--metric", default="rss",
                        choices=["rss", "heapUsed", "heapTotal", "external", "arrayBuffers"],
                        help="Metric to compare (default: rss)")
    parser.add_argument("--target-points", type=int, default=300,
                        help="Approximate number of points per series after downsampling (default: 300)")
    parser.add_argument("--output-plot", default=None, help="Path to save PNG plot")
    parser.add_argument("--output-json", default=None, help="Path to save reduced JSON")
    parser.add_argument("--no-align", action="store_true",
                        help="Do not trim to common max iteration (compare full ranges)")

    args = parser.parse_args()

    # Load all series
    loaded = []
    for path in args.files:
        label, iters, vals = load_series(path, args.metric)
        if not iters:
            print(f"Warning: no iterations for {path}, skipping.")
            continue
        loaded.append((label, iters, vals))

    if not loaded:
        raise SystemExit("No valid inputs.")

    # Align
    if not args.no_align:
        max_common, loaded = align_to_common_iteration(loaded)
        if max_common < 0:
            print("Warning: could not determine common max iteration.")
        else:
            print(f"Aligned to common max iteration: {max_common}")

    # Downsample
    downsampled = []
    for label, iters, vals in loaded:
        di, dv = bucket_downsample(iters, vals, args.target_points)
        downsampled.append((label, di, dv))

    # Outputs
    if args.output_plot:
        plot_series(args.metric, downsampled, args.output_plot)
        print(f"Wrote plot: {args.output_plot}")

    if args.output_json:
        write_reduced_json(args.output_json, args.metric, downsampled)
        print(f"Wrote reduced JSON: {args.output_json}")

    # If neither is specified, print a quick textual summary
    if not args.output_plot and not args.output_json:
        print(f"Metric: {args.metric} (MB)")
        for label, iters, vals in downsampled:
            if vals:
                print(f"{label}: {len(vals)} points, min={min(vals):.1f} MB, max={max(vals):.1f} MB")
            else:
                print(f"{label}: no data after downsampling")

if __name__ == "__main__":
    main()
