#!/usr/bin/env python3
import os
import re
import math
import argparse
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
COL_SEEDPARTS_REC = "Total Seed Parts Received"
COL_SAMPLES_REC = "Total Sample Received"
COL_SEEDPART_ARR = "Seed Part Arrival Times"
COL_SAMPLE_ARR = "Sample Arrival Times"


def safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def parse_times_cell_fast(x) -> np.ndarray:
    if pd.isna(x):
        return np.empty(0, dtype=float)
    if isinstance(x, (int, float, np.integer, np.floating)):
        return np.array([float(x)], dtype=float) if np.isfinite(x) else np.empty(0, dtype=float)

    s = str(x).strip()
    if not s or s in ("[]", "nan", "None"):
        return np.empty(0, dtype=float)

    if s[0] == "[" and s[-1] == "]":
        s = s[1:-1].strip()
        if not s:
            return np.empty(0, dtype=float)

    s = s.replace(";", ",")
    arr = np.fromstring(s, sep=",", dtype=float)
    if arr.size == 0:
        return np.empty(0, dtype=float)
    return arr[np.isfinite(arr)]


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
            if not (csvout and topics_part and k_part):
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
        raise RuntimeError(f"No matching CSVs found under: {root_dir}")
    return df.sort_values(["csvout","topics","k","sa","mr","seed"]).reset_index(drop=True)


def kth_completion_time(arrivals_abs: np.ndarray, k: int, block_start: float) -> float:
    if arrivals_abs.size < k:
        return np.nan
    arrivals_abs = np.sort(arrivals_abs)
    t = float(arrivals_abs[k - 1]) - float(block_start)
    return t if np.isfinite(t) and t >= 0 else np.nan


def compute_seed_stats_one_csv(path: str, sa: int, das_target: int, slot_ms: int):
    # find final time
    tdf = pd.read_csv(path, usecols=["Time"], low_memory=False)
    times = safe_numeric(tdf["Time"]).dropna()
    if len(times) == 0:
        return {}

    final_time = int(times.max())
    block_start = max(0, final_time - slot_ms)

    seeding_target = max(1, int(math.ceil(sa / 2.0)))

    usecols = [
        COL_TIME, COL_REQ_SENT,
        COL_SEEDPARTS_REC, COL_SAMPLES_REC,
        COL_SEEDPART_ARR, COL_SAMPLE_ARR
    ]
    df = pd.read_csv(path, usecols=lambda c: c in usecols, low_memory=False)
    df_t = df[safe_numeric(df[COL_TIME]) == final_time].copy()
    if df_t.empty:
        return {}

    total_nodes = len(df_t)
    req = safe_numeric(df_t.get(COL_REQ_SENT, pd.Series([np.nan]*total_nodes)))
    eligible_mask = (req > 0)

    eligible = int(eligible_mask.sum())
    excluded = int(total_nodes - eligible)
    excluded_fraction = excluded / total_nodes if total_nodes else np.nan

    if eligible == 0:
        return {
            "final_time": final_time,
            "block_start": block_start,
            "seeding_target": seeding_target,
            "eligible_nodes": 0,
            "excluded_nodes": excluded,
            "excluded_fraction": excluded_fraction,
            "seeding_success": np.nan,
            "das_success": np.nan,
            "__seed_times_nodelevel": np.empty(0),
            "__das_times_nodelevel": np.empty(0),
        }

    dfe = df_t.loc[eligible_mask].copy()

    seedparts = safe_numeric(dfe.get(COL_SEEDPARTS_REC, pd.Series(dtype=float))).to_numpy(dtype=float)
    samples = safe_numeric(dfe.get(COL_SAMPLES_REC, pd.Series(dtype=float))).to_numpy(dtype=float)

    seeding_success = float(np.mean(seedparts >= seeding_target)) if seedparts.size else np.nan
    das_success = float(np.mean(samples >= das_target)) if samples.size else np.nan

    seed_times = []
    das_times = []

    ok_seed_mask = (seedparts >= seeding_target) if seedparts.size else np.zeros(len(dfe), dtype=bool)
    if ok_seed_mask.any():
        for v in dfe.loc[ok_seed_mask, COL_SEEDPART_ARR].values:
            arr = parse_times_cell_fast(v)
            tcomp = kth_completion_time(arr, seeding_target, block_start)
            if np.isfinite(tcomp):
                seed_times.append(tcomp)

    ok_das_mask = (samples >= das_target) if samples.size else np.zeros(len(dfe), dtype=bool)
    if ok_das_mask.any():
        for v in dfe.loc[ok_das_mask, COL_SAMPLE_ARR].values:
            arr = parse_times_cell_fast(v)
            tcomp = kth_completion_time(arr, das_target, block_start)
            if np.isfinite(tcomp):
                das_times.append(tcomp)

    return {
        "final_time": final_time,
        "block_start": block_start,
        "seeding_target": seeding_target,
        "eligible_nodes": eligible,
        "excluded_nodes": excluded,
        "excluded_fraction": excluded_fraction,
        "seeding_success": seeding_success,
        "das_success": das_success,
        "__seed_times_nodelevel": np.asarray(seed_times, dtype=float),
        "__das_times_nodelevel": np.asarray(das_times, dtype=float),
    }


def median_iqr(x: pd.Series):
    a = safe_numeric(x).dropna().to_numpy()
    if a.size == 0:
        return (np.nan, np.nan, np.nan)
    return (float(np.median(a)), float(np.quantile(a, 0.25)), float(np.quantile(a, 0.75)))


def plot_success_rates_png(cfg: pd.DataFrame, out_png: str, title: str):
    seed_med, seed_q25, seed_q75 = median_iqr(cfg["seeding_success_med"])
    das_med,  das_q25,  das_q75  = median_iqr(cfg["das_success_med"])

    fig, ax = plt.subplots(figsize=(7.5, 5))
    labels = ["Seeding", "DAS"]
    meds = np.array([seed_med, das_med], dtype=float)
    q25 = np.array([seed_q25, das_q25], dtype=float)
    q75 = np.array([seed_q75, das_q75], dtype=float)

    x = np.arange(len(labels))
    yerr = np.vstack([meds - q25, q75 - meds])
    ax.bar(x, meds, yerr=yerr, capsize=6)

    rng = np.random.default_rng(0)
    jitter = (rng.random(len(cfg)) - 0.5) * 0.18
    ax.scatter(np.full(len(cfg), x[0]) + jitter, cfg["seeding_success_med"].to_numpy(), s=14, alpha=0.6)
    ax.scatter(np.full(len(cfg), x[1]) + jitter, cfg["das_success_med"].to_numpy(), s=14, alpha=0.6)

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(-0.02, 1.05)
    ax.set_ylabel("Success rate (per-config median over seeds)")
    ax.set_title(title)
    ax.grid(True, axis="y", alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_png, dpi=220)
    plt.close(fig)


def _quantiles(arr: np.ndarray):
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return (np.nan, np.nan, np.nan)
    return (
        float(np.quantile(arr, 0.50)),
        float(np.quantile(arr, 0.95)),
        float(np.quantile(arr, 0.99)),
    )


# def plot_cdf_with_markers(arr: np.ndarray, out_png: str, title: str, xlabel: str):
#     """
#     CDF + vertical lines at median / p95 / p99 with labels.
#     Also writes the numeric values in a small text box.
#     """
#     arr = arr[np.isfinite(arr)]
#     fig, ax = plt.subplots(figsize=(7.4, 5.2))
#
#     if arr.size == 0:
#         ax.text(0.5, 0.5, "No completion-time data", ha="center", va="center", transform=ax.transAxes)
#         ax.set_title(title)
#         ax.set_xlabel(xlabel)
#         ax.set_ylabel("CDF")
#         ax.grid(True, alpha=0.3)
#         fig.tight_layout()
#         fig.savefig(out_png, dpi=220)
#         plt.close(fig)
#         return
#
#     arr = np.sort(arr)
#     y = np.arange(1, arr.size + 1) / arr.size
#     ax.plot(arr, y)
#
#     med, p95, p99 = _quantiles(arr)
#
#     # vertical markers
#     ax.axvline(med, linestyle="--", linewidth=2.0, label=f"median = {med:.1f} ms")
#     ax.axvline(p95, linestyle="--", linewidth=2.0, label=f"p95 = {p95:.1f} ms")
#     ax.axvline(p99, linestyle="--", linewidth=2.0, label=f"p99 = {p99:.1f} ms")
#
#     # keep lines visible: extend y-limit slightly
#     ax.set_ylim(0, 1.02)
#
#     # small textbox
#     txt = f"median: {med:.1f} ms\np95: {p95:.1f} ms\np99: {p99:.1f} ms\nn={arr.size}"
#     ax.text(
#         0.98, 0.02, txt,
#         transform=ax.transAxes,
#         ha="right", va="bottom",
#         fontsize=10,
#         bbox=dict(boxstyle="round,pad=0.3", alpha=0.15)
#     )
#
#     ax.set_title(title)
#     ax.set_xlabel(xlabel)
#     ax.set_ylabel("CDF")
#     ax.grid(True, alpha=0.3)
#     ax.legend(loc="lower right")
#
#     fig.tight_layout()
#     fig.savefig(out_png, dpi=220)
#     plt.close(fig)
#

def plot_cdf_with_markers(arr: np.ndarray, out_png: str, title: str, xlabel: str):
    """
    CDF + vertical lines at median / p95 / p99 with 3 distinct colors + labels.
    """
    arr = arr[np.isfinite(arr)]
    fig, ax = plt.subplots(figsize=(7.4, 5.2))

    if arr.size == 0:
        ax.text(0.5, 0.5, "No completion-time data", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("CDF")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(out_png, dpi=220)
        plt.close(fig)
        return

    arr = np.sort(arr)
    y = np.arange(1, arr.size + 1) / arr.size
    ax.plot(arr, y)  # leave default color for the CDF line

    med = float(np.quantile(arr, 0.50))
    p95 = float(np.quantile(arr, 0.95))
    p99 = float(np.quantile(arr, 0.99))

    # 3 distinct colors
    c_med = "tab:green"
    c_p95 = "tab:orange"
    c_p99 = "tab:red"

    ax.axvline(med, linestyle="--", linewidth=2.2, color=c_med, label=f"median = {med:.1f} ms")
    ax.axvline(p95, linestyle="--", linewidth=2.2, color=c_p95, label=f"p95 = {p95:.1f} ms")
    ax.axvline(p99, linestyle="--", linewidth=2.2, color=c_p99, label=f"p99 = {p99:.1f} ms")

    ax.set_ylim(0, 1.02)

    txt = f"median: {med:.1f} ms\np95: {p95:.1f} ms\np99: {p99:.1f} ms\nn={arr.size}"
    ax.text(
        0.98, 0.02, txt,
        transform=ax.transAxes,
        ha="right", va="bottom",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.3", alpha=0.15)
    )

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("CDF")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right")

    fig.tight_layout()
    fig.savefig(out_png, dpi=220)
    plt.close(fig)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root folder containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("--out", default="mr0_baseline_256_sa4_sa8", help="Output folder")
    ap.add_argument("--topics", type=int, default=256, help="Only keep TOPICS_AMOUNT == this value (default 256)")
    ap.add_argument("--sa", nargs="+", type=int, default=[4, 8], help="Only keep these SA values (default 4 8)")
    ap.add_argument("--das-target", type=int, default=73, help="DAS target samples")
    ap.add_argument("--slot-ms", type=int, default=12000, help="Slot length in ms (block_start = Time - slot_ms)")
    ap.add_argument("--mr0", type=float, default=0.0, help="Which MR value to treat as baseline (default 0.0)")
    args = ap.parse_args()

    ensure_dir(args.out)

    idx = build_file_index(args.root)
    idx = idx[idx["topics"] == args.topics]
    idx = idx[idx["sa"].isin(args.sa)]

    if idx.empty:
        raise RuntimeError(f"No files found with TOPICS_AMOUNT_{args.topics} and SA in {args.sa}")

    mr0_df = idx[np.isclose(idx["mr"].to_numpy(dtype=float), float(args.mr0))]
    if mr0_df.empty:
        raise RuntimeError(f"No files found for MR={args.mr0} with TOPICS={args.topics} and SA in {args.sa}.")

    print(f"Files at MR=0 with TOPICS={args.topics} and SA in {args.sa}:", len(mr0_df))

    rows = []
    seed_times_global = []
    das_times_global = []

    for r in mr0_df.itertuples(index=False):
        stats = compute_seed_stats_one_csv(
            path=r.path,
            sa=int(r.sa),
            das_target=int(args.das_target),
            slot_ms=int(args.slot_ms),
        )
        if not stats:
            continue

        seed_times_global.append(stats.pop("__seed_times_nodelevel", np.empty(0)))
        das_times_global.append(stats.pop("__das_times_nodelevel", np.empty(0)))

        stats.update({
            "csvout": r.csvout,
            "topics": r.topics,
            "k": r.k,
            "sa": r.sa,
            "mr": r.mr,
            "seed": r.seed,
            "path": r.path
        })
        rows.append(stats)

    per_seed = pd.DataFrame(rows)
    per_seed_csv = os.path.join(args.out, "mr0_per_seed.csv")
    per_seed.to_csv(per_seed_csv, index=False)
    print("Saved:", per_seed_csv)

    group_cols = ["csvout", "topics", "k", "sa", "mr"]
    cfg = (
        per_seed
        .groupby(group_cols, as_index=False)
        .agg(
            seeding_success_med=("seeding_success", "median"),
            das_success_med=("das_success", "median"),
            excluded_fraction_med=("excluded_fraction", "median"),
            seeds=("seed", "nunique"),
        )
        .sort_values(group_cols)
        .reset_index(drop=True)
    )
    cfg_csv = os.path.join(args.out, "mr0_per_config_median_over_seeds.csv")
    cfg.to_csv(cfg_csv, index=False)
    print("Saved:", cfg_csv)

    # Success plot
    out_success_png = os.path.join(args.out, "MR0_success_rates.png")
    plot_success_rates_png(
        cfg,
        out_success_png,
        title=f"MR=0 baseline success rates (TOPICS={args.topics}, SA in {args.sa})"
    )
    print("Saved:", out_success_png)

    # CDF plots with annotations
    seed_times_all = np.concatenate(seed_times_global) if seed_times_global else np.empty(0)
    das_times_all = np.concatenate(das_times_global) if das_times_global else np.empty(0)

    out_seed_cdf = os.path.join(args.out, "cdf_seeding_completion_time_mr0.png")
    plot_cdf_with_markers(
        seed_times_all,
        out_seed_cdf,
        title=f"CDF: Seeding completion time @MR=0 (TOPICS={args.topics}, SA in {args.sa}, final block)",
        xlabel="Completion time (ms since block start)",
    )
    print("Saved:", out_seed_cdf)

    out_das_cdf = os.path.join(args.out, "cdf_das_completion_time_mr0.png")
    plot_cdf_with_markers(
        das_times_all,
        out_das_cdf,
        title=f"CDF: DAS completion time @MR=0 (TOPICS={args.topics}, SA in {args.sa}, final block)",
        xlabel="Completion time (ms since block start)",
    )
    print("Saved:", out_das_cdf)

    # print numbers too
    if np.isfinite(seed_times_all).any():
        med, p95, p99 = _quantiles(seed_times_all)
        print(f"Seeding completion @MR=0: median={med:.2f}ms, p95={p95:.2f}ms, p99={p99:.2f}ms (n={np.isfinite(seed_times_all).sum()})")
    if np.isfinite(das_times_all).any():
        med, p95, p99 = _quantiles(das_times_all)
        print(f"DAS completion @MR=0:     median={med:.2f}ms, p95={p95:.2f}ms, p99={p99:.2f}ms (n={np.isfinite(das_times_all).sum()})")

    print("\nDone.")


if __name__ == "__main__":
    main()
