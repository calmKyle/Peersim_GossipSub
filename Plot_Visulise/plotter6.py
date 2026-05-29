#!/usr/bin/env python3
"""
plot_all_configs_fast.py

Merged-by-config plotting for PeerSim GossipSub sharding simulation outputs.

Reads folder structure like:
  CSVOut*/TOPICS_AMOUNT_<topics>/K_<k>/output_results_MR_<mr>_SA_<sa>_seed_<seed>.0.csv

Groups by config: (csvout, topics, k, mr, sa)
Merges across seeds using MEDIAN per-node per-block.
Outputs PNGs similar to your example images, one set per config.

Speed improvements:
- Multiprocessing per-config (ProcessPoolExecutor)
- Fast parsing for arrival-time list cells using numpy.fromstring
- Downsampling / caps (cap per-node samples, cap flattened points)
- Resume support: skips configs that already wrote _DONE.txt

Run:
  python3 plot_all_configs_fast.py --root . --out merged_plots --workers 8

Recommended faster knobs:
  --cap-per-node-samples 300 --max-flat 100000
"""

import os
import re
import math
import time
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import warnings
from pandas.errors import DtypeWarning


# Use non-interactive backend (important for multiprocessing)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ----------------------------
# File pattern
# ----------------------------
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)


# ----------------------------
# Helpers
# ----------------------------
def fmt_secs(s: float) -> str:
    if not np.isfinite(s):
        return "inf"
    s = int(max(0, s))
    return f"{s//60}m{s%60:02d}s" if s >= 60 else f"{s}s"


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)


def mr_str(mr: float) -> str:
    # filesystem friendly
    return str(mr).replace(".", "p")


def get_times(df: pd.DataFrame) -> list[int]:
    return sorted(safe_numeric(df["Time"]).dropna().astype(int).unique().tolist())


def block_start(time_t: int, slot_ms: int, start_method: str, prev_t: int | None = None) -> int:
    if start_method == "minus_slot":
        return max(0, int(time_t) - int(slot_ms))
    # prev_snapshot method
    return 0 if prev_t is None else int(prev_t)


def ecdf_xy(arr: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if arr.size == 0:
        return np.array([]), np.array([])
    arr = np.sort(arr)
    y = np.arange(1, arr.size + 1)
    return arr, y


# ----------------------------
# FAST parser for list-ish cells
# ----------------------------
def parse_times_cell_fast(x) -> np.ndarray:
    """
    Fast parse for a cell containing:
      - NaN / "" / "[]"
      - scalar number
      - "[1,2,3]" or "1,2,3" or "1;2;3"
    Returns np.ndarray(float).
    """
    if pd.isna(x):
        return np.empty(0, dtype=float)

    if isinstance(x, (int, float, np.integer, np.floating)):
        if np.isfinite(x):
            return np.array([float(x)], dtype=float)
        return np.empty(0, dtype=float)

    s = str(x).strip()
    if not s or s in ("[]", "nan", "None"):
        return np.empty(0, dtype=float)

    # strip brackets if present
    if s[0] == "[" and s[-1] == "]":
        s = s[1:-1].strip()
        if not s:
            return np.empty(0, dtype=float)

    # normalize separators
    s = s.replace(";", ",")

    arr = np.fromstring(s, sep=",", dtype=float)
    if arr.size == 0:
        return np.empty(0, dtype=float)

    arr = arr[np.isfinite(arr)]
    return arr


# ----------------------------
# Index all CSVs
# ----------------------------
def build_file_index(root_dir: str) -> pd.DataFrame:
    rows = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            m = FILENAME_RE.match(fn)
            if not m:
                continue

            parts = dirpath.replace("\\", "/").split("/")
            csvout = next((p for p in parts if p.startswith("CSVOut")), None)
            topics_part = next((p for p in parts if p.startswith("TOPICS_AMOUNT_")), None)
            k_part = next((p for p in parts if p.startswith("K_")), None)
            if not (topics_part and k_part):
                # allow selecting a specific folder without CSVOut*, but still need TOPICS/K to exist
                continue

            topics = int(topics_part.split("_")[-1])
            k = int(k_part.split("_")[-1])
            mr = float(m.group("mr"))
            sa = int(m.group("sa"))
            seed = int(m.group("seed"))

            rows.append({
                "csvout": csvout or "CUSTOM",
                "topics": topics,
                "k": k,
                "mr": mr,
                "sa": sa,
                "seed": seed,
                "path": os.path.join(dirpath, fn),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"No matching CSVs found under: {root_dir}")
    return df.sort_values(["csvout", "topics", "k", "mr", "sa", "seed"]).reset_index(drop=True)


# ----------------------------
# Loading minimal columns per seed
# ----------------------------
NEEDED_COLS = [
    "Time", "Node ID",
    "Duplicated_Data", "Shards_Amount",
    "Avg Bandwidth",
    "Seed Part Arrival Times",
    "Sample Arrival Times",
]

def load_seed_df(path: str) -> pd.DataFrame:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always", DtypeWarning)

        df = pd.read_csv(
            path,
            usecols=lambda c: c in NEEDED_COLS,
            low_memory=False  # reduces mixed-type chunking issues
        )

        # If pandas raised a dtype warning, print the file
        dtype_warnings = [x for x in w if issubclass(x.category, DtypeWarning)]
        if dtype_warnings:
            print("\n[DtypeWarning] in file:", path)
            print("  message:", str(dtype_warnings[0].message))

            # Try to map the numeric index to the actual column name
            try:
                all_cols = pd.read_csv(path, nrows=0).columns.tolist()
                m = re.search(r"Columns? \(([^)]+)\)", str(dtype_warnings[0].message))
                if m:
                    idxs = [int(s.strip()) for s in m.group(1).split(",")]
                    for ci in idxs:
                        if 0 <= ci < len(all_cols):
                            print(f"  column index {ci} -> '{all_cols[ci]}'")
                        else:
                            print(f"  column index {ci} -> <out of range>")
            except Exception as e:
                print("  (Could not map column index to name):", e)

        return df


def seed_node_metrics_for_time(df_t: pd.DataFrame, start: int, cap_per_node_samples: int) -> pd.DataFrame:
    """
    Per row/node:
      dup_per_shard
      bandwidth
      seed_node_first: first seed-part arrival since block start
      seed_node_last : last  seed-part arrival since block start
      sample_node_mean: mean(sample arrivals) since block start
    """
    out = pd.DataFrame()
    if "Node ID" in df_t.columns:
        out["Node ID"] = df_t["Node ID"]
    else:
        out["Node ID"] = np.arange(len(df_t))

    # dup_per_shard
    if "Duplicated_Data" in df_t.columns and "Shards_Amount" in df_t.columns:
        dup = safe_numeric(df_t["Duplicated_Data"])
        shards = safe_numeric(df_t["Shards_Amount"])
        out["dup_per_shard"] = (dup / shards).replace([np.inf, -np.inf], np.nan)
    else:
        out["dup_per_shard"] = np.nan

    # bandwidth
    if "Avg Bandwidth" in df_t.columns:
        out["bandwidth"] = safe_numeric(df_t["Avg Bandwidth"])
    else:
        out["bandwidth"] = np.nan

    def node_minmax_from_col(col: str):
        """Return (first_arrival, last_arrival) arrays per node, relative to block start."""
        if col not in df_t.columns:
            n = len(df_t)
            return np.full(n, np.nan, dtype=float), np.full(n, np.nan, dtype=float)

        first = np.full(len(df_t), np.nan, dtype=float)
        last  = np.full(len(df_t), np.nan, dtype=float)

        vals = df_t[col].values
        for i, v in enumerate(vals):
            arr = parse_times_cell_fast(v)
            if arr.size == 0:
                continue

            arr.sort()
            f = float(arr[0]) - start
            l = float(arr[-1]) - start

            if np.isfinite(f) and f >= 0:
                first[i] = f
            if np.isfinite(l) and l >= 0:
                last[i] = l

        return first, last

    def node_mean_from_col(col: str, cap: int | None):
        if col not in df_t.columns:
            return np.full(len(df_t), np.nan, dtype=float)

        res = np.full(len(df_t), np.nan, dtype=float)
        vals = df_t[col].values

        for i, v in enumerate(vals):
            arr = parse_times_cell_fast(v)
            if arr.size == 0:
                continue
            if cap is not None and arr.size > cap:
                arr = arr[:cap]
            m = float(arr.mean()) - start
            if np.isfinite(m) and m >= 0:
                res[i] = m
        return res

    out["seed_node_first"], out["seed_node_last"] = node_minmax_from_col("Seed Part Arrival Times")
    out["sample_node_mean"] = node_mean_from_col("Sample Arrival Times", cap=cap_per_node_samples)

    return out


def flatten_arrivals(df_t: pd.DataFrame, col: str, start: int, max_points: int | None) -> np.ndarray:
    if col not in df_t.columns:
        return np.empty(0, dtype=float)

    chunks = []
    for v in df_t[col].values:
        arr = parse_times_cell_fast(v)
        if arr.size:
            chunks.append(arr)

    if not chunks:
        return np.empty(0, dtype=float)

    flat = np.concatenate(chunks).astype(float, copy=False)
    flat = flat[np.isfinite(flat)]
    flat = flat - start
    flat = flat[flat >= 0]

    if max_points is not None and flat.size > max_points:
        rng = np.random.default_rng(0)
        idx = rng.choice(flat.size, size=max_points, replace=False)
        flat = flat[idx]

    return flat


# ----------------------------
# Merge seeds: median over seeds, per node per time
# ----------------------------
def merge_seeds_median_nodewise(seed_paths, slot_ms, start_method, cap_per_node_samples, max_points_flat):
    seeds = [load_seed_df(p) for p in seed_paths]

    times = get_times(seeds[0])
    for s in seeds[1:]:
        t2 = set(get_times(s))
        times = [t for t in times if t in t2]
    times = sorted(times)
    if not times:
        raise RuntimeError("No common Time snapshots across seeds for this config.")

    node_medians = {}

    probs = np.linspace(0.0, 1.0, 200)
    seedpart_quantile_cdf = {}

    for i, t in enumerate(times):
        prev_t = times[i - 1] if i > 0 else None
        start = block_start(t, slot_ms, start_method, prev_t)

        per_seed_tables = []
        per_seed_q = []
        per_seed_n = []

        for s in seeds:
            df_t = s[safe_numeric(s["Time"]) == t]
            tab = seed_node_metrics_for_time(df_t, start=start, cap_per_node_samples=cap_per_node_samples)
            per_seed_tables.append(tab)

            flat = flatten_arrivals(df_t, "Seed Part Arrival Times", start=start, max_points=max_points_flat)
            if flat.size:
                flat.sort()
                per_seed_q.append(np.quantile(flat, probs, method="linear"))
                per_seed_n.append(int(flat.size))

        all_nodes = pd.concat(
            [t.assign(seed_idx=j) for j, t in enumerate(per_seed_tables)],
            ignore_index=True
        )

        med = (
            all_nodes
            .groupby("Node ID", as_index=False)[
                ["dup_per_shard", "seed_node_first", "seed_node_last", "sample_node_mean", "bandwidth"]
            ]
            .median(numeric_only=True)
        )

        node_medians[t] = med

        if per_seed_q:
            Q = np.vstack(per_seed_q)
            q_med = np.median(Q, axis=0)
            n_med = int(np.median(per_seed_n)) if per_seed_n else 0
            y_counts = probs * n_med
            seedpart_quantile_cdf[t] = (q_med, y_counts)
        else:
            seedpart_quantile_cdf[t] = (np.array([]), np.array([]))

    t_final = times[-1]
    bw_final = node_medians[t_final]["bandwidth"].dropna().to_numpy()

    return times, node_medians, bw_final, seedpart_quantile_cdf


# ----------------------------
# Plot outputs
# ----------------------------
def plot_bandwidth_distribution(bw, out_png):
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.hist(bw, bins=60)
    ax.set_title("Bandwidth distribution across nodes")
    ax.set_xlabel("Avg bandwidth per node")
    ax.set_ylabel("Number of nodes")
    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)


def plot_cdf_blocks_node_level(times, node_medians, out_png, threshold_ms):
    n = len(times)
    cols = 2 if n > 1 else 1
    rows = math.ceil(n / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(12, 12))
    axes = np.array(axes).reshape(-1)

    for i, t in enumerate(times):
        ax = axes[i]
        med = node_medians[t]

        seed_first = med.get("seed_node_first", pd.Series(dtype=float)).dropna().to_numpy()
        seed_last  = med.get("seed_node_last",  pd.Series(dtype=float)).dropna().to_numpy()
        samp_node  = med.get("sample_node_mean", pd.Series(dtype=float)).dropna().to_numpy()

        xF, yF = ecdf_xy(seed_first)
        xL, yL = ecdf_xy(seed_last)
        xS, yS = ecdf_xy(samp_node)

        if xF.size:
            ax.plot(xF, yF, label="Seed first arrival (per node)")
        if xL.size:
            ax.plot(xL, yL, label="Seed last arrival (per node)")
            max_seed = float(np.max(xL))
            ax.axvline(max_seed, linestyle="--", label=f"Max seed last ({max_seed:.1f} ms)")

        if xS.size:
            ax.plot(xS, yS, label="Sample arrival (node mean)")

        ax.axvline(threshold_ms, color="red", linestyle="--", label=f"Threshold {threshold_ms} ms")

        ax.set_title(f"CDF of Node Distribution (Slot {i+1})")
        ax.set_xlabel("Time (ms) - Relative to Slot Start")
        ax.set_ylabel("Number of Nodes")
        ax.legend()

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)


def plot_duplication_hist_per_block(times, node_medians, out_png):
    n = len(times)
    cols = 3
    rows = math.ceil(n / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(12, 8), sharex=True)
    axes = np.array(axes).reshape(-1)

    for i, t in enumerate(times):
        ax = axes[i]
        x = node_medians[t]["dup_per_shard"].dropna().to_numpy()
        ax.hist(x, bins=30)
        ax.set_title(f"Slot {i+1}")
        ax.set_xlabel("Avg duplicated data per shard (per node)")
        ax.set_ylabel("Number of nodes")
        if x.size:
            med = float(np.median(x))
            ax.text(0.98, 0.92, f"median = {med:.2f}", ha="right", va="top", transform=ax.transAxes)

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.suptitle("Duplication per shard distribution per slot (median over seeds)")
    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)


def plot_duplication_violin_per_block(times, node_medians, out_png):
    n = len(times)
    cols = 3
    rows = math.ceil(n / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(12, 8), sharex=True)
    axes = np.array(axes).reshape(-1)

    for i, t in enumerate(times):
        ax = axes[i]
        x = node_medians[t]["dup_per_shard"].dropna().to_numpy()
        if x.size:
            ax.violinplot([x], vert=False, showmeans=False, showextrema=False)
            med = float(np.median(x))
            ax.scatter([med], [1], marker="D")
            ax.text(0.98, 0.92, f"median = {med:.2f}", ha="right", va="top", transform=ax.transAxes)

        ax.set_title(f"Slot {i+1}")
        ax.set_xlabel("Avg duplicated data per shard (per node)")
        ax.set_yticks([])

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.suptitle("Duplication per shard distribution per block (violin, median over seeds)")
    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)


def plot_overall_duplication_violin(times, node_medians, out_png):
    per_time = []
    for t in times:
        tmp = node_medians[t][["Node ID", "dup_per_shard"]].copy()
        tmp["Time"] = t
        per_time.append(tmp)

    all_df = pd.concat(per_time, ignore_index=True)
    node_avg = all_df.groupby("Node ID")["dup_per_shard"].mean().dropna().to_numpy()
    if node_avg.size == 0:
        return

    med = float(np.median(node_avg))

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.violinplot([node_avg], vert=False, showmeans=False, showextrema=False)
    ax.scatter([med], [1], marker="D", label="Median")
    ax.set_yticks([])
    ax.set_title("Distribution of avg duplicated data per shard (all slots)")
    ax.set_xlabel("Avg duplicated data per shard (per node)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)


# ----------------------------
# One config runner
# ----------------------------
def run_one_config(seed_paths, out_dir, topics, k, mr, sa,
                   slot_ms, start_method, threshold_ms,
                   cap_per_node_samples, max_points_flat):
    os.makedirs(out_dir, exist_ok=True)

    times, node_medians, bw_final, seedpart_qcdf = merge_seeds_median_nodewise(
        seed_paths=seed_paths,
        slot_ms=slot_ms,
        start_method=start_method,
        cap_per_node_samples=cap_per_node_samples,
        max_points_flat=max_points_flat
    )

    tag = f"TOPICS_{topics}_K_{k}_MR_{mr_str(mr)}_SA_{sa}"

    plot_bandwidth_distribution(bw_final, os.path.join(out_dir, f"{tag}_bandwidth_distribution.png"))
    plot_cdf_blocks_node_level(times, node_medians, os.path.join(out_dir, f"{tag}_CDF_Blocks.png"), threshold_ms)

    # Optional: only plot seed-part-per-block if the helper exists
    if "plot_cdf_seed_part_per_block" in globals():
        plot_cdf_seed_part_per_block(
            times,
            seedpart_qcdf,
            os.path.join(out_dir, f"{tag}_cdf_seed_part_arrival_per_block.png")
        )

    plot_duplication_hist_per_block(times, node_medians, os.path.join(out_dir, f"{tag}_duplication_per_shard_per_block.png"))
    plot_duplication_violin_per_block(times, node_medians, os.path.join(out_dir, f"{tag}_duplication_per_shard_per_block_violin.png"))
    plot_overall_duplication_violin(times, node_medians, os.path.join(out_dir, f"{tag}_duplication_per_shard_violin.png"))


def process_one_group(task):
    """
    Worker entrypoint for multiprocessing.
    """
    (seed_paths, out_dir, topics, k, mr, sa,
     slot_ms, start_method, threshold_ms, cap_per_node_samples, max_points_flat) = task

    done_flag = os.path.join(out_dir, "_DONE.txt")
    if os.path.exists(done_flag):
        return ("skipped", out_dir)

    run_one_config(
        seed_paths=seed_paths,
        out_dir=out_dir,
        topics=topics,
        k=k,
        mr=mr,
        sa=sa,
        slot_ms=slot_ms,
        start_method=start_method,
        threshold_ms=threshold_ms,
        cap_per_node_samples=cap_per_node_samples,
        max_points_flat=max_points_flat
    )

    with open(done_flag, "w", encoding="utf-8") as f:
        f.write("ok\n")

    return ("ok", out_dir)


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root folder containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("-d", "--dir", dest="root", help="Alias for --root (choose a specific folder).")
    ap.add_argument("--out", default="merged_plots", help="Output folder for PNGs")
    ap.add_argument("--slot-ms", type=int, default=12_000, help="Slot length in ms (default 12000)")
    ap.add_argument("--start-method", choices=["minus_slot", "prev_snapshot"], default="minus_slot",
                    help="How to compute block start for relative times")
    ap.add_argument("--threshold-ms", type=int, default=4000, help="Threshold line in ms for CDF blocks")
    ap.add_argument("--cap-per-node-samples", type=int, default=300,
                    help="Cap parsed samples per node for Sample Arrival Times (lower=faster)")
    ap.add_argument("--max-flat", type=int, default=100_000,
                    help="Downsample cap for flattened seed-part arrivals per block (lower=faster)")
    ap.add_argument("--workers", type=int, default=0, help="Num processes (0 = CPU-1)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    idx = build_file_index(args.root)
    print("Indexed files:", len(idx))
    print(idx.head())

    group_cols = ["csvout", "topics", "k", "mr", "sa"]
    groups = list(idx.groupby(group_cols))
    print(f"\nFound {len(groups)} configs (each should have ~5 seeds).")

    tasks = []
    for (csvout, topics, k, mr, sa), g in groups:
        seed_paths = g.sort_values("seed")["path"].tolist()
        out_dir = os.path.join(args.out, f"MR_{mr_str(mr)}", f"TOPICS_{topics}", f"K_{k}", f"SA_{sa}")
        tasks.append((seed_paths, out_dir, topics, k, mr, sa,
                      args.slot_ms, args.start_method, args.threshold_ms,
                      args.cap_per_node_samples, args.max_flat))

    workers = args.workers
    if workers <= 0:
        workers = max(1, (os.cpu_count() or 2) - 1)

    print("Using workers:", workers)
    t0 = time.time()

    done = 0
    ok = 0
    skipped = 0

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(process_one_group, t) for t in tasks]

        for f in as_completed(futs):
            status, where = f.result()
            done += 1
            ok += (status == "ok")
            skipped += (status == "skipped")

            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(tasks) - done) / rate if rate > 0 else float("inf")

            print(f"[{done}/{len(tasks)}] {status} | ok={ok} skipped={skipped} | rate={rate:.2f} cfg/s | ETA={fmt_secs(eta)}")

    print("\nDone. Plots saved under:", args.out)


if __name__ == "__main__":
    main()
