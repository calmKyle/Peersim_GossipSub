#!/usr/bin/env python3
import os
import re
import time
import math
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ----------------------------
# File pattern
# ----------------------------
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)
SLOT_MS = 12_000


# ----------------------------
# Progress helpers
# ----------------------------
def fmt_secs(s: float) -> str:
    if not np.isfinite(s):
        return "inf"
    s = int(max(0, s))
    return f"{s//60}m{s%60:02d}s" if s >= 60 else f"{s}s"


# ----------------------------
# Index files based on your folder structure
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
# Read final snapshot metrics from ONE CSV
#   - two-pass: first read Time col to find max time
#   - then read needed cols and compute stats at final time
# ----------------------------
def summarize_one_csv_final(path: str) -> dict:
    # pass 1: max time
    tcol = pd.read_csv(path, usecols=["Time"])
    times = pd.to_numeric(tcol["Time"], errors="coerce").dropna()
    if len(times) == 0:
        raise RuntimeError(f"No valid Time in {path}")
    final_time = int(times.max())

    # slot start = block start (because the timestamp is block finished time)
    # slot_start = max(0, int(times[block_index]) - SLOT_MS)
    slot_start = max(0, final_time - SLOT_MS)

    # pass 2: read only useful columns
    usecols = [
        "Time",
        "Duplicated_Data",
        "Shards_Amount",
        "Avg Bandwidth",
        "Avg Sample RTT",
        "Avg Seed Part RTT",
        "Duplicated_IHAVE",
    ]
    d = pd.read_csv(path, usecols=lambda c: c in usecols)
    d = d[pd.to_numeric(d["Time"], errors="coerce") == final_time].copy()
    if d.empty:
        raise RuntimeError(f"Final time {final_time} not found after filtering in {path}")

    out = {"final_time": final_time}

    # dup_per_shard (same as before)
    if "Duplicated_Data" in d.columns and "Shards_Amount" in d.columns:
        dup = pd.to_numeric(d["Duplicated_Data"], errors="coerce")
        shards = pd.to_numeric(d["Shards_Amount"], errors="coerce")
        dup_per_shard = (dup / shards).replace([np.inf, -np.inf], np.nan).dropna()
        out["dup_per_shard_mean_final"] = float(dup_per_shard.mean()) if len(dup_per_shard) else np.nan
        out["dup_per_shard_median_final"] = float(dup_per_shard.median()) if len(dup_per_shard) else np.nan
    else:
        out["dup_per_shard_mean_final"] = np.nan
        out["dup_per_shard_median_final"] = np.nan

    def _mean(col):
        if col not in d.columns:
            return np.nan
        x = pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        return float(x.mean()) if len(x) else np.nan

    def _median(col):
        if col not in d.columns:
            return np.nan
        x = pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        return float(x.median()) if len(x) else np.nan

    # bandwidth etc (same)
    out["bandwidth_mean_final"] = _mean("Avg Bandwidth")
    out["dup_data_mean_final"] = _mean("Duplicated_Data")
    out["dup_ihave_mean_final"] = _mean("Duplicated_IHAVE")

    def _median_since_slot_start(col):
        if col not in d.columns:
            return np.nan
        x = pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(x) == 0:
            return np.nan

        # Heuristic: if it looks like absolute time, convert to relative
        if float(x.median()) > SLOT_MS * 1.5:
            x = x - slot_start

        # keep sane range (optional, but helps)
        x = x[(x >= 0) & (x <= SLOT_MS)]
        return float(x.median()) if len(x) else np.nan

    out["sample_rtt_median_final"] = _median_since_slot_start("Avg Sample RTT")
    out["seed_part_rtt_median_final"] = _median_since_slot_start("Avg Seed Part RTT")

    return out


# ----------------------------
# Aggregate across seeds (median per config)
# ----------------------------
def aggregate_median_over_seeds(seed_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["csvout","topics","k","mr","sa"]
    value_cols = [c for c in seed_df.columns if c not in group_cols + ["seed","path"]]
    agg = (
        seed_df
        .groupby(group_cols, as_index=False)[value_cols]
        .median(numeric_only=True)
        .sort_values(group_cols)
        .reset_index(drop=True)
    )
    return agg


# ----------------------------
# Plot A: Bubble matrix (SA vs TOPICS, size=K, facet=MR, color=metric)
# ----------------------------
def plot_bubble_matrix(cfg_df: pd.DataFrame, metric: str, out_png: str, higher_is_better: bool):
    mrs = sorted(cfg_df["mr"].unique().tolist())
    sas = sorted(cfg_df["sa"].unique().tolist())
    topics = sorted(cfg_df["topics"].unique().tolist())
    ks = sorted(cfg_df["k"].unique().tolist())

    if metric not in cfg_df.columns:
        raise RuntimeError(f"Metric '{metric}' not found in aggregated df columns: {list(cfg_df.columns)}")

    vals = cfg_df[metric].replace([np.inf, -np.inf], np.nan).dropna().to_numpy()
    if len(vals) == 0:
        raise RuntimeError(f"No numeric values for metric '{metric}'.")

    vmin, vmax = float(np.nanmin(vals)), float(np.nanmax(vals))
    if not higher_is_better:
        # If lower is better, we still color by the metric,
        # but you'll interpret darker/lighter accordingly.
        pass

    nrows = len(mrs)
    fig, axes = plt.subplots(nrows, 1, figsize=(11, 3.5*nrows), sharex=True, sharey=True)
    if nrows == 1:
        axes = [axes]

    # bubble size mapping from K
    def size_for_k(k):
        return 80 + 70*(k**1.15)

    cmap = plt.cm.viridis
    norm = plt.Normalize(vmin=vmin, vmax=vmax)

    for ax, mr in zip(axes, mrs):
        sub = cfg_df[np.isclose(cfg_df["mr"], mr)].copy()
        if sub.empty:
            ax.set_axis_off()
            continue

        sc = ax.scatter(
            sub["sa"].to_numpy(),
            sub["topics"].to_numpy(),
            s=sub["k"].apply(size_for_k).to_numpy(),
            c=sub[metric].to_numpy(),
            cmap=cmap,
            norm=norm,
            alpha=0.9,
            edgecolors="black",
            linewidths=0.3
        )

        ax.set_title(f"Bubble matrix | MR={mr} | color={metric} | size=K")
        ax.set_xlabel("SA")
        ax.set_ylabel("TOPICS")
        ax.set_xticks(sas)
        ax.set_yticks(topics)
        ax.grid(True, linewidth=0.3, alpha=0.35)

        # Size legend for K (draw 3 example bubbles)
        handles = []
        labels = []
        for k in ks:
            handles.append(ax.scatter([], [], s=size_for_k(k), edgecolors="black", facecolors="none"))
            labels.append(f"K={k}")
        ax.legend(handles, labels, title="Replication", loc="upper right", fontsize=8)

    cbar = fig.colorbar(plt.cm.ScalarMappable(norm=norm, cmap=cmap), ax=axes, fraction=0.02, pad=0.02)
    cbar.set_label(metric)

    fig.tight_layout()
    fig.savefig(out_png, dpi=200)
    print(f"Saved: {out_png}")


# ----------------------------
# Plot B: Interaction lines (metric vs SA, lines=K, panels=TOPICS) per MR
# ----------------------------
def plot_interaction_lines(cfg_df: pd.DataFrame, metric: str, out_dir: str):
    mrs = sorted(cfg_df["mr"].unique().tolist())
    sas = sorted(cfg_df["sa"].unique().tolist())
    topics_list = sorted(cfg_df["topics"].unique().tolist())
    ks = sorted(cfg_df["k"].unique().tolist())

    for mr in mrs:
        sub_mr = cfg_df[np.isclose(cfg_df["mr"], mr)]
        if sub_mr.empty:
            continue

        n = len(topics_list)
        rows = math.ceil(n / 2)
        cols = 2 if n > 1 else 1

        fig, axes = plt.subplots(rows, cols, figsize=(12, 3.6*rows), sharex=True)
        axes = np.array(axes).reshape(-1)

        for i, topics in enumerate(topics_list):
            ax = axes[i]
            sub = sub_mr[sub_mr["topics"] == topics]
            if sub.empty:
                ax.set_axis_off()
                continue

            for k in ks:
                s = sub[sub["k"] == k].sort_values("sa")
                if s.empty:
                    continue
                ax.plot(s["sa"].to_numpy(), s[metric].to_numpy(), marker="o", label=f"K={k}")

            ax.set_title(f"MR={mr} | TOPICS={topics}")
            ax.set_xticks(sas)
            ax.set_xlabel("SA")
            ax.set_ylabel(metric)
            ax.grid(True, linewidth=0.3, alpha=0.35)
            if i == 0:
                ax.legend(fontsize=8)

        # remove unused axes
        for j in range(i+1, len(axes)):
            fig.delaxes(axes[j])

        fig.suptitle(f"Interaction: {metric} vs SA (lines=K) | MR={mr}", y=1.02)
        fig.tight_layout()
        out_png = os.path.join(out_dir, f"lines_{metric}_MR_{mr}.png")
        fig.savefig(out_png, dpi=200)
        plt.close(fig)
        print(f"Saved: {out_png}")


# ----------------------------
# Plot C: Parallel coordinates (interactive HTML) if plotly is installed
# ----------------------------
def plot_parallel_coordinates_html(cfg_df: pd.DataFrame, metric: str, out_html: str):
    try:
        import plotly.express as px
    except Exception:
        print("Plotly not installed. Skipping parallel coordinates.")
        print("Install with: pip install plotly")
        return

    df = cfg_df.copy()
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[metric])

    fig = px.parallel_coordinates(
        df,
        dimensions=["mr", "sa", "k", "topics"],
        color=metric,
        color_continuous_scale="Viridis",
        labels={"mr":"MR","sa":"SA","k":"K","topics":"TOPICS"}
    )
    fig.write_html(out_html)
    print(f"Saved: {out_html}")


# ----------------------------
# MAIN
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root directory containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("--out", default="viz_out", help="Output directory for plots/csv")
    ap.add_argument("--metric", default="dup_per_shard_mean_final",
                    help="Metric column to visualize (from aggregated table)")
    ap.add_argument("--higher_is_better", action="store_true",
                    help="Use this if higher metric means better (affects interpretation only)")
    ap.add_argument("--log_every", type=int, default=5, help="Progress print every N files")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    idx = build_file_index(args.root)
    print(f"Indexed files: {len(idx)}")
    print(idx.head())

    # Summarize each seed file
    seed_rows = []
    t0 = time.time()
    total = len(idx)

    print(f"\nSummarizing {total} CSV files (final snapshot metrics)...")
    for i, r in enumerate(idx.itertuples(index=False), start=1):
        if i == 1 or i % args.log_every == 0 or i == total:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (total - i) / rate if rate > 0 else float("inf")
            print(f"[{i:>4}/{total}] elapsed={fmt_secs(elapsed)} rate={rate:.2f} files/s ETA={fmt_secs(eta)}")
            print(f"   -> {r.csvout} TOPICS={r.topics} K={r.k} MR={r.mr} SA={r.sa} seed={r.seed}")
            print(f"      file: {r.path}")

        s = summarize_one_csv_final(r.path)
        s.update({
            "csvout": r.csvout,
            "topics": r.topics,
            "k": r.k,
            "mr": r.mr,
            "sa": r.sa,
            "seed": r.seed,
            "path": r.path
        })
        seed_rows.append(s)

    seed_df = pd.DataFrame(seed_rows)
    seed_csv = os.path.join(args.out, "summary_per_seed_final.csv")
    seed_df.to_csv(seed_csv, index=False)
    print(f"\nSaved per-seed summary: {seed_csv}")

    # Aggregate median over seeds
    cfg_df = aggregate_median_over_seeds(seed_df)
    cfg_csv = os.path.join(args.out, "summary_median_over_seeds_final.csv")
    cfg_df.to_csv(cfg_csv, index=False)
    print(f"Saved aggregated config summary: {cfg_csv}")

    # Show what exists
    print("\nUnique values:")
    print(" MR:", sorted(cfg_df["mr"].unique().tolist()))
    print(" SA:", sorted(cfg_df["sa"].unique().tolist()))
    print(" K :", sorted(cfg_df["k"].unique().tolist()))
    print(" TOPICS:", sorted(cfg_df["topics"].unique().tolist()))
    print("\nAvailable metric columns:")
    print([c for c in cfg_df.columns if c not in ["csvout","topics","k","mr","sa"]])

    if args.metric not in cfg_df.columns:
        raise RuntimeError(f"Metric '{args.metric}' not found. Choose one from the list above.")

    # Plots
    plot_bubble_matrix(
        cfg_df=cfg_df,
        metric=args.metric,
        out_png=os.path.join(args.out, f"bubble_{args.metric}.png"),
        higher_is_better=args.higher_is_better
    )

    plot_interaction_lines(cfg_df=cfg_df, metric=args.metric, out_dir=args.out)

    plot_parallel_coordinates_html(
        cfg_df=cfg_df,
        metric=args.metric,
        out_html=os.path.join(args.out, f"parallel_{args.metric}.html")
    )

    # Rank configs (quick "best/worst" table)
    rank_df = cfg_df.sort_values(args.metric, ascending=not args.higher_is_better).reset_index(drop=True)
    rank_csv = os.path.join(args.out, f"ranked_by_{args.metric}.csv")
    rank_df.to_csv(rank_csv, index=False)
    print(f"Saved ranking: {rank_csv}")


if __name__ == "__main__":
    main()

