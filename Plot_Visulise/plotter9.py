#!/usr/bin/env python3
"""
duplication_vs_mr.py

Reads CSVs under:
  CSVOut*/TOPICS_AMOUNT_<topics>/K_<k>/output_results_MR_<mr>_SA_<sa>_seed_<seed>.0.csv

Computes duplication metrics at FINAL snapshot (max Time), optionally excluding
malicious/non-participating nodes (Total Sample Req Sent == 0).

Outputs:
  out/
    summary_dup_per_seed.csv
    summary_dup_median_iqr.csv
    lines_per_k/<metric>/lines_dup_vs_mr_K_<k>.png
    heatmaps/<metric>/heatmaps_TOPICS_<t>_K_<k>.png

Metric options:
  - data_per_shard   = Duplicated_Data / Shards_Amount   (recommended)
  - ihave_per_shard  = Duplicated_IHAVE / Shards_Amount
  - data             = Duplicated_Data
  - ihave            = Duplicated_IHAVE
"""

import os
import re
import argparse
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ----------------------------
# Filename pattern
# ----------------------------
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

# Columns in your CSV
COL_TIME = "Time"
COL_REQ_SENT = "Total Sample Req Sent"      # for exclusion rule
COL_DUP_IHAVE = "Duplicated_IHAVE"
COL_DUP_DATA = "Duplicated_Data"
COL_SHARDS = "Shards_Amount"


# ----------------------------
# Helpers
# ----------------------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)

def quantile_safe(x: np.ndarray, q: float) -> float:
    x = x[np.isfinite(x)]
    if x.size == 0:
        return np.nan
    return float(np.quantile(x, q))


# ----------------------------
# Index CSVs
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
    return df.sort_values(["csvout","topics","k","mr","sa","seed"]).reset_index(drop=True)


# ----------------------------
# Summarize one CSV at FINAL snapshot
# ----------------------------
def summarize_dup_one_csv(path: str, metric: str, exclude_req_sent_zero: bool) -> dict:
    """
    Returns duplication summary over nodes at the final time.
    If exclude_req_sent_zero is True, excludes nodes with Total Sample Req Sent == 0.

    Per-run scalar recommended for plotting:
      - dup_node_median (median across eligible nodes)

    Also returns:
      - dup_node_mean, dup_p90, dup_p99
      - eligible/excluded counts
    """
    # Find final time quickly
    tdf = pd.read_csv(path, usecols=[COL_TIME], low_memory=False)
    times = safe_numeric(tdf[COL_TIME]).dropna()
    if len(times) == 0:
        return {
            "final_time": np.nan,
            "eligible_nodes": 0,
            "excluded_nodes": 0,
            "excluded_fraction": np.nan,
            "dup_node_median": np.nan,
            "dup_node_mean": np.nan,
            "dup_p90": np.nan,
            "dup_p99": np.nan,
        }

    final_time = int(times.max())

    needed = [COL_TIME, COL_DUP_IHAVE, COL_DUP_DATA, COL_SHARDS]
    if exclude_req_sent_zero:
        needed.append(COL_REQ_SENT)

    df = pd.read_csv(path, usecols=lambda c: c in needed, low_memory=False)
    df_final = df[safe_numeric(df[COL_TIME]) == final_time].copy()
    if df_final.empty:
        return {
            "final_time": final_time,
            "eligible_nodes": 0,
            "excluded_nodes": 0,
            "excluded_fraction": np.nan,
            "dup_node_median": np.nan,
            "dup_node_mean": np.nan,
            "dup_p90": np.nan,
            "dup_p99": np.nan,
        }

    total_nodes = len(df_final)

    if exclude_req_sent_zero:
        req = safe_numeric(df_final.get(COL_REQ_SENT, pd.Series([np.nan] * total_nodes)))
        eligible_mask = (req > 0)
    else:
        eligible_mask = np.ones(total_nodes, dtype=bool)

    eligible_nodes = int(np.sum(eligible_mask))
    excluded_nodes = int(total_nodes - eligible_nodes)
    excluded_fraction = excluded_nodes / total_nodes if total_nodes else np.nan

    if eligible_nodes == 0:
        return {
            "final_time": final_time,
            "eligible_nodes": 0,
            "excluded_nodes": excluded_nodes,
            "excluded_fraction": excluded_fraction,
            "dup_node_median": np.nan,
            "dup_node_mean": np.nan,
            "dup_p90": np.nan,
            "dup_p99": np.nan,
        }

    dfe = df_final.loc[eligible_mask].copy()

    dup_ihave = safe_numeric(dfe.get(COL_DUP_IHAVE, pd.Series(dtype=float))).to_numpy(dtype=float)
    dup_data  = safe_numeric(dfe.get(COL_DUP_DATA,  pd.Series(dtype=float))).to_numpy(dtype=float)
    shards    = safe_numeric(dfe.get(COL_SHARDS,    pd.Series(dtype=float))).to_numpy(dtype=float)

    # Build per-node metric array
    if metric == "data_per_shard":
        valid = np.isfinite(dup_data) & np.isfinite(shards) & (shards > 0)
        vals = dup_data[valid] / shards[valid]
    elif metric == "ihave_per_shard":
        valid = np.isfinite(dup_ihave) & np.isfinite(shards) & (shards > 0)
        vals = dup_ihave[valid] / shards[valid]
    elif metric == "data":
        vals = dup_data[np.isfinite(dup_data)]
    elif metric == "ihave":
        vals = dup_ihave[np.isfinite(dup_ihave)]
    else:
        raise ValueError(f"Unknown metric: {metric}")

    if vals.size == 0:
        return {
            "final_time": final_time,
            "eligible_nodes": eligible_nodes,
            "excluded_nodes": excluded_nodes,
            "excluded_fraction": excluded_fraction,
            "dup_node_median": np.nan,
            "dup_node_mean": np.nan,
            "dup_p90": np.nan,
            "dup_p99": np.nan,
        }

    return {
        "final_time": final_time,
        "eligible_nodes": eligible_nodes,
        "excluded_nodes": excluded_nodes,
        "excluded_fraction": excluded_fraction,
        "dup_node_median": float(np.median(vals)),
        "dup_node_mean": float(np.mean(vals)),
        "dup_p90": quantile_safe(vals, 0.90),
        "dup_p99": quantile_safe(vals, 0.99),
    }


# ----------------------------
# Aggregate across seeds: median + IQR
# ----------------------------
def aggregate_median_iqr(seed_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["csvout", "topics", "k", "mr", "sa"]

    def agg(col: str) -> pd.DataFrame:
        g = seed_df.groupby(group_cols)[col]
        out = g.agg(
            median="median",
            q25=lambda x: x.quantile(0.25),
            q75=lambda x: x.quantile(0.75),
        ).reset_index()
        out = out.rename(columns={
            "median": f"{col}_median",
            "q25": f"{col}_q25",
            "q75": f"{col}_q75",
        })
        return out

    cols = ["dup_node_median", "dup_node_mean", "dup_p90", "dup_p99", "excluded_fraction"]
    out = None
    for c in cols:
        tmp = agg(c)
        out = tmp if out is None else out.merge(tmp, on=group_cols, how="outer")

    return out.sort_values(group_cols).reset_index(drop=True)


# ----------------------------
# Plots
# ----------------------------


def load_node_overhead_final(csv_path):
    df = pd.read_csv(csv_path, usecols=lambda c: c in [COL_TIME, COL_REQ_SENT, COL_DUP_DATA, COL_SHARDS, COL_BW], low_memory=False)
    tmax = int(safe_numeric(df[COL_TIME]).dropna().max())
    dft = df[safe_numeric(df[COL_TIME]) == tmax].copy()

    # exclude faulty nodes
    elig = safe_numeric(dft[COL_REQ_SENT]) > 0
    dft = dft.loc[elig]

    dup = safe_numeric(dft[COL_DUP_DATA]).to_numpy(float)
    shards = safe_numeric(dft[COL_SHARDS]).to_numpy(float)
    bw = safe_numeric(dft[COL_BW]).to_numpy(float)

    valid = np.isfinite(dup) & np.isfinite(shards) & (shards > 0)
    dup_per_shard = (dup[valid] / shards[valid])
    dup_per_shard = dup_per_shard[np.isfinite(dup_per_shard)]
    bw = bw[np.isfinite(bw)]
    return dup_per_shard, bw

def violin_over_mr(file_index_df, topics, sa, k, out_png):
    """
    file_index_df: dataframe with columns [topics, sa, k, mr, path]
    """
    sub = file_index_df[(file_index_df["topics"] == topics) & (file_index_df["sa"] == sa) & (file_index_df["k"] == k)]
    mrs = sorted(sub["mr"].unique().tolist())

    dup_groups = []
    bw_groups = []

    for mr in mrs:
        paths = sub[sub["mr"] == mr]["path"].tolist()
        dup_all = []
        bw_all = []
        for p in paths:  # across seeds
            d, b = load_node_overhead_final(p)
            if d.size: dup_all.append(d)
            if b.size: bw_all.append(b)
        dup_groups.append(np.concatenate(dup_all) if dup_all else np.array([]))
        bw_groups.append(np.concatenate(bw_all) if bw_all else np.array([]))

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Duplication violin
    axes[0].violinplot(dup_groups, showmeans=False, showextrema=False)
    axes[0].set_title(f"Duplication per shard vs MR (TOPICS={topics}, SA={sa}, K={k})")
    axes[0].set_xlabel("MR")
    axes[0].set_ylabel("Dup per shard")
    axes[0].set_xticks(np.arange(1, len(mrs)+1))
    axes[0].set_xticklabels([str(x) for x in mrs])

    # Bandwidth violin
    axes[1].violinplot(bw_groups, showmeans=False, showextrema=False)
    axes[1].set_title(f"Bandwidth per node vs MR (TOPICS={topics}, SA={sa}, K={k})")
    axes[1].set_xlabel("MR")
    axes[1].set_ylabel("Avg bandwidth")
    axes[1].set_xticks(np.arange(1, len(mrs)+1))
    axes[1].set_xticklabels([str(x) for x in mrs])

    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)
    print("Saved:", out_png)

def plot_lines_dup_vs_mr(agg_df: pd.DataFrame, out_dir: str, ycol: str, title_prefix: str):
    """
    Faceted: rows=TOPICS, cols=SA, one PNG per K.
    ycol should be something like 'dup_node_median_median' (median over seeds of node-median).
    """
    ensure_dir(out_dir)

    topics_vals = sorted(agg_df["topics"].unique().tolist())
    sa_vals = sorted(agg_df["sa"].unique().tolist())
    k_vals = sorted(agg_df["k"].unique().tolist())

    yq25 = ycol.replace("_median", "_q25")
    yq75 = ycol.replace("_median", "_q75")

    for k in k_vals:
        dfk = agg_df[agg_df["k"] == k].copy()
        if dfk.empty:
            continue

        fig, axes = plt.subplots(
            len(topics_vals), len(sa_vals),
            figsize=(4 * len(sa_vals), 3.2 * len(topics_vals)),
            sharex=True, sharey=False, squeeze=False
        )

        for i, topics in enumerate(topics_vals):
            for j, sa in enumerate(sa_vals):
                ax = axes[i][j]
                sub = dfk[(dfk["topics"] == topics) & (dfk["sa"] == sa)].sort_values("mr")
                if sub.empty:
                    ax.set_axis_off()
                    continue

                ax.plot(sub["mr"], sub[ycol], marker="o", linewidth=2.3, markersize=6)
                if yq25 in sub.columns and yq75 in sub.columns:
                    ax.fill_between(sub["mr"], sub[yq25], sub[yq75], alpha=0.2)

                ax.set_title(f"TOPICS={topics} | SA={sa}")
                ax.grid(True, alpha=0.3)

                if i == len(topics_vals) - 1:
                    ax.set_xlabel("Malicious Rate (MR)")
                if j == 0:
                    ax.set_ylabel(title_prefix)

        fig.suptitle(f"{title_prefix} vs MR (median ± IQR over seeds) | K={k}", y=1.02)
        fig.tight_layout()
        out_png = os.path.join(out_dir, f"lines_dup_vs_mr_K_{k}.png")
        fig.savefig(out_png, dpi=200)
        plt.close(fig)
        print("Saved:", out_png)


def _heatmap(ax, pivot, title, annotate=True):
    if pivot.empty:
        ax.set_axis_off()
        return None

    im = ax.imshow(pivot.values, origin="lower", aspect="auto")
    ax.set_title(title)
    ax.set_xlabel("MR")
    ax.set_ylabel("SA")

    ax.set_xticks(np.arange(pivot.shape[1]))
    ax.set_xticklabels([str(x) for x in pivot.columns.tolist()])
    ax.set_yticks(np.arange(pivot.shape[0]))
    ax.set_yticklabels([str(int(x)) for x in pivot.index.tolist()])

    if annotate:
        for r in range(pivot.shape[0]):
            for c in range(pivot.shape[1]):
                val = pivot.values[r, c]
                if np.isfinite(val):
                    ax.text(c, r, f"{val:.2f}", ha="center", va="center", fontsize=8)

    return im


def plot_heatmaps(agg_df: pd.DataFrame, out_dir: str, value_col: str, title_prefix: str):
    ensure_dir(out_dir)
    topics_vals = sorted(agg_df["topics"].unique().tolist())
    k_vals = sorted(agg_df["k"].unique().tolist())

    for topics in topics_vals:
        for k in k_vals:
            sub = agg_df[(agg_df["topics"] == topics) & (agg_df["k"] == k)].copy()
            if sub.empty:
                continue

            pv = sub.pivot_table(index="sa", columns="mr", values=value_col, aggfunc="mean").sort_index().sort_index(axis=1)

            fig, ax = plt.subplots(1, 1, figsize=(6.5, 4.8))
            im = _heatmap(ax, pv, f"{title_prefix} | TOPICS={topics} K={k}", annotate=True)
            if im is not None:
                fig.colorbar(im, ax=ax, fraction=0.05, pad=0.03)

            fig.tight_layout()
            out_png = os.path.join(out_dir, f"heatmaps_TOPICS_{topics}_K_{k}.png")
            fig.savefig(out_png, dpi=200)
            plt.close(fig)
            print("Saved:", out_png)


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root folder containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("--out", default="dup_out", help="Output directory")
    ap.add_argument("--metric", default="data_per_shard",
                    choices=["data_per_shard", "ihave_per_shard", "data", "ihave"],
                    help="Which duplication metric to plot")
    ap.add_argument("--include-malicious", action="store_true",
                    help="If set, do NOT exclude nodes with Total Sample Req Sent==0")
    ap.add_argument("--log-every", type=int, default=25, help="Progress print every N files")
    args = ap.parse_args()

    ensure_dir(args.out)

    idx = build_file_index(args.root)
    print("Indexed files:", len(idx))
    print(idx.head(3))

    exclude_req0 = (not args.include_malicious)

    # Per-seed summaries
    rows = []
    total = len(idx)
    for i, r in enumerate(idx.itertuples(index=False), start=1):
        if i == 1 or i % args.log_every == 0 or i == total:
            print(f"[{i:>4}/{total}] {r.path}")

        s = summarize_dup_one_csv(
            path=r.path,
            metric=args.metric,
            exclude_req_sent_zero=exclude_req0
        )
        s.update({
            "csvout": r.csvout,
            "topics": r.topics,
            "k": r.k,
            "mr": r.mr,
            "sa": r.sa,
            "seed": r.seed,
            "path": r.path
        })
        rows.append(s)

    seed_df = pd.DataFrame(rows)
    seed_csv = os.path.join(args.out, "summary_dup_per_seed.csv")
    seed_df.to_csv(seed_csv, index=False)
    print("Saved:", seed_csv)

    agg_df = aggregate_median_iqr(seed_df)
    agg_csv = os.path.join(args.out, "summary_dup_median_iqr.csv")
    agg_df.to_csv(agg_csv, index=False)
    print("Saved:", agg_csv)

    # Plot choice: use median-over-seeds of node-median (robust)
    ycol = "dup_node_median_median"
    title_prefix = f"Duplication ({args.metric})"

    lines_dir = os.path.join(args.out, "lines_per_k", args.metric)
    plot_lines_dup_vs_mr(agg_df, out_dir=lines_dir, ycol=ycol, title_prefix=title_prefix)

    heat_dir = os.path.join(args.out, "heatmaps", args.metric)
    plot_heatmaps(agg_df, out_dir=heat_dir, value_col=ycol, title_prefix=title_prefix)

    violin_dir = os.path.join(args.out, "violin", args.metric)
    violin_over_mr(agg_df, out_dir=violin_dir, value_col=ycol, title_prefix=title_prefix)

    print("\nDone. Outputs in:", args.out)
    if exclude_req0:
        print("Note: excluded nodes where Total Sample Req Sent == 0 (malicious/non-participating).")
    else:
        print("Note: included all nodes (did NOT exclude Total Sample Req Sent == 0).")


if __name__ == "__main__":
    main()

