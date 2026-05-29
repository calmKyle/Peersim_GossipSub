#!/usr/bin/env python3
"""
overhead_two_plots_from_raw_minmax.py

Reads ALL raw CSVs under:
  CSVOut*/TOPICS_AMOUNT_<topics>/K_<k>/output_results_MR_<mr>_SA_<sa>_seed_<seed>.0.csv

Computes overhead at FINAL snapshot (Time == max(Time)):
  - duplication per segment: Duplicated_Data / Shards_Amount  (per node)
  - bandwidth per node: Avg Bandwidth  (per node)
Excludes faulty nodes by default (Total Sample Req Sent == 0).

Aggregation:
  1) Per CSV run -> node-median metrics
  2) Per config (csvout,topics,k,sa,mr) -> median over seeds
  3) Per MR -> aggregate across configs:
        median, min, max (+ optional q25/q75)

Outputs (separate plots):
  ONE_GRAPH_duplication_vs_mr_min_max_median.png
  ONE_GRAPH_bandwidth_vs_mr_min_max_median.png
  (optional) ONE_GRAPH_faulty_fraction_vs_mr_min_max_median.png

Run:
  python3 overhead_two_plots_from_raw_minmax.py --root . --out thesis_plots --workers 8
"""

import os
import re
import time
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

COL_TIME = "Time"
COL_REQ_SENT = "Total Sample Req Sent"
COL_DUP_DATA = "Duplicated_Data"
COL_SHARDS = "Shards_Amount"
COL_BW = "Avg Bandwidth"


def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)

def fmt_secs(s: float) -> str:
    if not np.isfinite(s):
        return "inf"
    s = int(max(0, s))
    return f"{s//60}m{s%60:02d}s" if s >= 60 else f"{s}s"


# ----------------------------
# indexing
# ----------------------------
def build_file_index(root_dir: str, max_unmatched_preview: int = 25) -> pd.DataFrame:
    rows = []
    unmatched = []

    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if not fn.endswith(".csv"):
                continue

            m = FILENAME_RE.match(fn)
            if not m:
                if len(unmatched) < max_unmatched_preview:
                    unmatched.append(os.path.join(dirpath, fn))
                continue

            parts = dirpath.replace("\\", "/").split("/")
            csvout = next((p for p in parts if p.startswith("CSVOut")), None)
            topics_part = next((p for p in parts if p.startswith("TOPICS_AMOUNT_")), None)
            k_part = next((p for p in parts if p.startswith("K_")), None)

            if not (csvout and topics_part and k_part):
                if len(unmatched) < max_unmatched_preview:
                    unmatched.append(os.path.join(dirpath, fn))
                continue

            topics = int(topics_part.split("_")[-1])
            k = int(k_part.split("_")[-1])
            mr = float(m.group("mr"))
            sa = int(m.group("sa"))
            seed = int(m.group("seed"))

            rows.append({
                "csvout": csvout,
                "topics": topics,
                "k": k,
                "mr": mr,
                "sa": sa,
                "seed": seed,
                "path": os.path.join(dirpath, fn),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        msg = f"No matching CSVs found under: {root_dir}\n"
        msg += "Example unmatched .csv files:\n" + "\n".join(unmatched[:10])
        raise RuntimeError(msg)

    print(f"Indexed files: {len(df)}")
    if unmatched:
        print(f"Note: {len(unmatched)} .csv files did NOT match the expected filename/folder pattern.")
        print("First few unmatched:")
        for p in unmatched[:10]:
            print("  ", p)

    return df.sort_values(["csvout","topics","k","sa","mr","seed"]).reset_index(drop=True)


# ----------------------------
# final snapshot reading (chunked 2-pass)
# ----------------------------
def find_final_time(csv_path: str, chunksize: int) -> int | None:
    max_t = None
    for chunk in pd.read_csv(csv_path, usecols=[COL_TIME], chunksize=chunksize, low_memory=False):
        t = safe_numeric(chunk[COL_TIME]).dropna()
        if len(t) == 0:
            continue
        m = int(t.max())
        max_t = m if (max_t is None or m > max_t) else max_t
    return max_t


def summarize_one_csv_final(csv_path: str, exclude_faulty: bool, chunksize: int) -> dict:
    final_t = find_final_time(csv_path, chunksize=chunksize)
    if final_t is None:
        return {
            "final_time": np.nan,
            "eligible_nodes": 0,
            "excluded_nodes": 0,
            "excluded_fraction": np.nan,
            "dup_node_median": np.nan,
            "bw_node_median": np.nan,
        }

    usecols = [COL_TIME, COL_DUP_DATA, COL_SHARDS, COL_BW]
    if exclude_faulty:
        usecols.append(COL_REQ_SENT)

    dup_vals = []
    bw_vals = []
    eligible = 0
    excluded = 0

    for chunk in pd.read_csv(csv_path, usecols=lambda c: c in usecols, chunksize=chunksize, low_memory=False):
        t = safe_numeric(chunk.get(COL_TIME, pd.Series(dtype=float)))
        chunk = chunk.loc[t == final_t]
        if chunk.empty:
            continue

        n = len(chunk)
        if exclude_faulty:
            req = safe_numeric(chunk.get(COL_REQ_SENT, pd.Series([np.nan]*n)))
            mask = (req > 0)
            excluded += int((~mask).sum())
            chunk = chunk.loc[mask]

        if chunk.empty:
            continue

        eligible += len(chunk)

        dup = safe_numeric(chunk.get(COL_DUP_DATA, pd.Series(dtype=float))).to_numpy(dtype=float)
        shards = safe_numeric(chunk.get(COL_SHARDS, pd.Series(dtype=float))).to_numpy(dtype=float)
        bw = safe_numeric(chunk.get(COL_BW, pd.Series(dtype=float))).to_numpy(dtype=float)

        valid = np.isfinite(dup) & np.isfinite(shards) & (shards > 0)
        dps = (dup[valid] / shards[valid])
        dps = dps[np.isfinite(dps)]
        if dps.size:
            dup_vals.append(dps)

        bw = bw[np.isfinite(bw)]
        if bw.size:
            bw_vals.append(bw)

    dup_all = np.concatenate(dup_vals) if dup_vals else np.empty(0)
    bw_all = np.concatenate(bw_vals) if bw_vals else np.empty(0)

    total_nodes = eligible + excluded
    excluded_fraction = (excluded / total_nodes) if total_nodes else np.nan

    return {
        "final_time": final_t,
        "eligible_nodes": int(eligible),
        "excluded_nodes": int(excluded),
        "excluded_fraction": float(excluded_fraction) if np.isfinite(excluded_fraction) else np.nan,
        "dup_node_median": float(np.median(dup_all)) if dup_all.size else np.nan,
        "bw_node_median": float(np.median(bw_all)) if bw_all.size else np.nan,
    }


def _worker(task):
    row_dict, exclude_faulty, chunksize = task
    s = summarize_one_csv_final(row_dict["path"], exclude_faulty=exclude_faulty, chunksize=chunksize)
    s.update(row_dict)
    return s


# ----------------------------
# aggregation
# ----------------------------
def aggregate_seed_median(per_file_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["csvout", "topics", "k", "sa", "mr"]
    return (
        per_file_df
        .groupby(group_cols, as_index=False)
        .agg(
            dup_cfg_median=("dup_node_median", "median"),
            bw_cfg_median=("bw_node_median", "median"),
            excluded_fraction=("excluded_fraction", "median"),
            seeds=("seed", "nunique"),
        )
        .sort_values(group_cols)
        .reset_index(drop=True)
    )


def aggregate_across_configs_by_mr(cfg_df: pd.DataFrame, col: str, also_iqr: bool) -> pd.DataFrame:
    rows = []
    for mr, g in cfg_df.groupby("mr"):
        x = pd.to_numeric(g[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().to_numpy()
        if x.size == 0:
            rows.append({"mr": mr, "median": np.nan, "min": np.nan, "max": np.nan, "q25": np.nan, "q75": np.nan, "n_cfg": 0})
        else:
            rows.append({
                "mr": mr,
                "median": float(np.median(x)),
                "min": float(np.min(x)),
                "max": float(np.max(x)),
                "q25": float(np.quantile(x, 0.25)) if also_iqr else np.nan,
                "q75": float(np.quantile(x, 0.75)) if also_iqr else np.nan,
                "n_cfg": int(x.size),
            })
    return pd.DataFrame(rows).sort_values("mr").reset_index(drop=True)


# ----------------------------
# plotting (single axis)
# ----------------------------
def plot_single_metric(curve: pd.DataFrame, out_png: str, title: str, ylabel: str, show_iqr: bool):
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))

    x = curve["mr"].to_numpy()
    med = curve["median"].to_numpy()
    mn = curve["min"].to_numpy()
    mx = curve["max"].to_numpy()

    ax.plot(x, med, marker="o", linewidth=2.6, markersize=6, label="Median")
    ax.plot(x, mn, linestyle="--", linewidth=2.0, label="Min")
    ax.plot(x, mx, linestyle="--", linewidth=2.0, label="Max")

    if show_iqr:
        q25 = curve["q25"].to_numpy()
        q75 = curve["q75"].to_numpy()
        ax.fill_between(x, q25, q75, alpha=0.18, label="IQR (25–75%)")

    ax.set_title(title)
    ax.set_xlabel("Malicious Rate (MR)")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best")

    fig.tight_layout()
    fig.savefig(out_png, dpi=220)
    plt.close(fig)
    print("Saved:", out_png)


# ----------------------------
# main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root folder containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("--out", default="thesis_plots", help="Output folder")
    ap.add_argument("--workers", type=int, default=0, help="Processes (0 = CPU-1)")
    ap.add_argument("--chunksize", type=int, default=200_000, help="CSV read chunksize")
    ap.add_argument("--include-faulty", action="store_true", help="Include faulty nodes (ReqSent==0)")
    ap.add_argument("--with-faulty-plot", action="store_true", help="Also output faulty fraction plot")
    ap.add_argument("--with-iqr", action="store_true", help="Also show IQR shading (25–75%)")
    ap.add_argument("--resume", action="store_true", help="If per_file_summary.csv exists, reuse it")
    args = ap.parse_args()

    ensure_dir(args.out)
    per_file_csv = os.path.join(args.out, "per_file_summary.csv")
    per_cfg_csv = os.path.join(args.out, "per_config_seed_median.csv")

    exclude_faulty = not args.include_faulty

    if args.resume and os.path.exists(per_file_csv):
        print("Resume: loading", per_file_csv)
        per_file_df = pd.read_csv(per_file_csv)
    else:
        idx = build_file_index(args.root)

        workers = args.workers
        if workers <= 0:
            workers = max(1, (os.cpu_count() or 2) - 1)
        print("Workers:", workers)

        tasks = [(r._asdict(), exclude_faulty, args.chunksize) for r in idx.itertuples(index=False)]
        rows = []

        t0 = time.time()
        total = len(tasks)

        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_worker, t) for t in tasks]
            for i, f in enumerate(as_completed(futs), start=1):
                rows.append(f.result())
                if i == 1 or i % 25 == 0 or i == total:
                    elapsed = time.time() - t0
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (total - i) / rate if rate > 0 else float("inf")
                    print(f"[{i}/{total}] {rate:.2f} files/s | ETA={fmt_secs(eta)}")

        per_file_df = pd.DataFrame(rows)
        per_file_df.to_csv(per_file_csv, index=False)
        print("Saved:", per_file_csv)

    cfg_df = aggregate_seed_median(per_file_df)
    cfg_df.to_csv(per_cfg_csv, index=False)
    print("Saved:", per_cfg_csv)

    # curves per MR across configs
    curve_dup = aggregate_across_configs_by_mr(cfg_df, "dup_cfg_median", also_iqr=args.with_iqr)
    curve_bw = aggregate_across_configs_by_mr(cfg_df, "bw_cfg_median", also_iqr=args.with_iqr)

    # separate outputs
    plot_single_metric(
        curve_dup,
        out_png=os.path.join(args.out, "ONE_GRAPH_duplication_vs_mr_min_max_median.png"),
        title="Network overhead vs MR — Duplication per segment (across configs)",
        ylabel="Duplication per shard\n(median/min/max across configs)",
        show_iqr=args.with_iqr,
    )
    plot_single_metric(
        curve_bw,
        out_png=os.path.join(args.out, "ONE_GRAPH_bandwidth_vs_mr_min_max_median.png"),
        title="Network overhead vs MR — Bandwidth per node (across configs)",
        ylabel="Avg bandwidth per node\n(median/min/max across configs)",
        show_iqr=args.with_iqr,
    )

    if args.with_faulty_plot:
        curve_faulty = aggregate_across_configs_by_mr(cfg_df, "excluded_fraction", also_iqr=args.with_iqr)
        plot_single_metric(
            curve_faulty,
            out_png=os.path.join(args.out, "ONE_GRAPH_faulty_fraction_vs_mr_min_max_median.png"),
            title="Faulty / non-participating fraction vs MR (across configs)",
            ylabel="Excluded fraction\n(ReqSent==0)",
            show_iqr=args.with_iqr,
        )

    print("\nDone.")
    print("Faulty nodes:", "EXCLUDED (ReqSent==0)" if exclude_faulty else "INCLUDED")


if __name__ == "__main__":
    main()
