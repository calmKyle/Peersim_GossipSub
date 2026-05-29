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


# ----------------------------
# Filename pattern
# ----------------------------
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

# Columns from your CSV header
COL_TIME = "Time"
COL_SEEDPARTS = "Total Seed Parts Received"
COL_SAMPLES = "Total Sample Received"
COL_REQ_SENT = "Total Sample Req Sent"   # used to filter out malicious nodes


# ----------------------------
# Helpers
# ----------------------------
def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def mr_str(mr: float) -> str:
    return str(mr).replace(".", "p")


# ----------------------------
# Index CSVs under root
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
# Compute success at FINAL snapshot of a CSV (excluding malicious nodes)
# ----------------------------
def summarize_success_one_csv(path: str, sa: int, das_target: int) -> dict:
    """
    Final snapshot = max(Time).

    Eligibility rule:
      - If Total Sample Req Sent == 0, node is malicious/non-participating and EXCLUDED.

    Seeding success (NEW RULE):
      - A validator succeeds if Total Seed Parts Received >= ceil(SA/2)

    DAS success:
      - A validator succeeds if Total Sample Received >= das_target

    Outputs computed over eligible nodes only:
      - seeding_success_mean    : mean(min(seedparts / seeding_target, 1))
      - seeding_success_strict  : fraction(seedparts >= seeding_target)
      - das_success_mean        : mean(min(samples / das_target, 1))
      - das_success_strict      : fraction(samples >= das_target)

    Plus diagnostics:
      - total_nodes_final
      - eligible_nodes_final
      - excluded_nodes_final
      - excluded_fraction_final
      - seeding_target (ceil(SA/2))
    """
    # Find final time quickly
    tcol = pd.read_csv(path, usecols=[COL_TIME], low_memory=False)
    times = safe_numeric(tcol[COL_TIME]).dropna()
    if len(times) == 0:
        return {
            "final_time": np.nan,
            "total_nodes_final": 0,
            "eligible_nodes_final": 0,
            "excluded_nodes_final": 0,
            "excluded_fraction_final": np.nan,
            "seeding_target": max(1, int(math.ceil(sa / 2.0))),
            "seeding_success_mean": np.nan,
            "seeding_success_strict": np.nan,
            "das_success_mean": np.nan,
            "das_success_strict": np.nan,
        }
    final_time = int(times.max())

    # Read needed columns only (keeps it fast and avoids mixed-type warnings)
    usecols = [COL_TIME, COL_REQ_SENT, COL_SEEDPARTS, COL_SAMPLES]
    df = pd.read_csv(path, usecols=lambda c: c in usecols, low_memory=False)

    df_final = df[safe_numeric(df[COL_TIME]) == final_time].copy()
    if df_final.empty:
        return {
            "final_time": final_time,
            "total_nodes_final": 0,
            "eligible_nodes_final": 0,
            "excluded_nodes_final": 0,
            "excluded_fraction_final": np.nan,
            "seeding_target": max(1, int(math.ceil(sa / 2.0))),
            "seeding_success_mean": np.nan,
            "seeding_success_strict": np.nan,
            "das_success_mean": np.nan,
            "das_success_strict": np.nan,
        }

    total_nodes = int(len(df_final))

    # Eligibility mask: req_sent > 0
    req_sent = safe_numeric(df_final.get(COL_REQ_SENT, pd.Series([np.nan]*len(df_final))))
    eligible_mask = (req_sent > 0)

    eligible_nodes = int(eligible_mask.sum())
    excluded_nodes = total_nodes - eligible_nodes
    excluded_frac = (excluded_nodes / total_nodes) if total_nodes > 0 else np.nan

    # If no eligible nodes, success is undefined
    if eligible_nodes == 0:
        return {
            "final_time": final_time,
            "total_nodes_final": total_nodes,
            "eligible_nodes_final": eligible_nodes,
            "excluded_nodes_final": excluded_nodes,
            "excluded_fraction_final": excluded_frac,
            "seeding_target": max(1, int(math.ceil(sa / 2.0))),
            "seeding_success_mean": np.nan,
            "seeding_success_strict": np.nan,
            "das_success_mean": np.nan,
            "das_success_strict": np.nan,
        }

    # Filter to eligible nodes only
    df_e = df_final.loc[eligible_mask].copy()

    seedparts = safe_numeric(df_e.get(COL_SEEDPARTS, pd.Series(dtype=float))).dropna()
    samples = safe_numeric(df_e.get(COL_SAMPLES, pd.Series(dtype=float))).dropna()

    # NEW: seeding target = ceil(SA/2)
    seeding_target = max(1, int(math.ceil(sa / 2.0)))

    # Seeding success
    if len(seedparts):
        seed_ratio = np.clip(seedparts.to_numpy(dtype=float) / float(seeding_target), 0.0, 1.0)
        seeding_success_mean = float(np.mean(seed_ratio))
        seeding_success_strict = float(np.mean(seedparts.to_numpy(dtype=float) >= float(seeding_target)))
    else:
        seeding_success_mean = np.nan
        seeding_success_strict = np.nan

    # DAS success
    if len(samples):
        das_ratio = np.clip(samples.to_numpy(dtype=float) / float(das_target), 0.0, 1.0)
        das_success_mean = float(np.mean(das_ratio))
        das_success_strict = float(np.mean(samples.to_numpy(dtype=float) >= float(das_target)))
    else:
        das_success_mean = np.nan
        das_success_strict = np.nan

    return {
        "final_time": final_time,
        "total_nodes_final": total_nodes,
        "eligible_nodes_final": eligible_nodes,
        "excluded_nodes_final": excluded_nodes,
        "excluded_fraction_final": excluded_frac,
        "seeding_target": seeding_target,
        "seeding_success_mean": seeding_success_mean,
        "seeding_success_strict": seeding_success_strict,
        "das_success_mean": das_success_mean,
        "das_success_strict": das_success_strict,
    }


# ----------------------------
# Aggregate across seeds: median + IQR
# ----------------------------
def aggregate_median_iqr(seed_df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["csvout", "topics", "k", "mr", "sa"]

    def agg_metric(col: str) -> pd.DataFrame:
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

    metrics = [
        # success
        "seeding_success_mean",
        "seeding_success_strict",
        "das_success_mean",
        "das_success_strict",
        # diagnostics
        "eligible_nodes_final",
        "excluded_nodes_final",
        "excluded_fraction_final",
        # info
        "seeding_target",
    ]

    out = None
    for col in metrics:
        tmp = agg_metric(col)
        out = tmp if out is None else out.merge(tmp, on=group_cols, how="outer")

    return out.sort_values(group_cols).reset_index(drop=True)


# ----------------------------
# Plots
# ----------------------------
def plot_lines_success_vs_mr(agg_df: pd.DataFrame, out_dir: str, metric: str):
    ensure_dir(out_dir)

    topics_vals = sorted(agg_df["topics"].unique().tolist())
    sa_vals = sorted(agg_df["sa"].unique().tolist())
    k_vals = sorted(agg_df["k"].unique().tolist())

    seed_col = f"seeding_success_{metric}_median"
    seed_q25 = f"seeding_success_{metric}_q25"
    seed_q75 = f"seeding_success_{metric}_q75"

    das_col = f"das_success_{metric}_median"
    das_q25 = f"das_success_{metric}_q25"
    das_q75 = f"das_success_{metric}_q75"

    for k in k_vals:
        dfk = agg_df[agg_df["k"] == k].copy()
        if dfk.empty:
            continue

        fig, axes = plt.subplots(
            len(topics_vals), len(sa_vals),
            figsize=(4 * len(sa_vals), 3.2 * len(topics_vals)),
            sharex=True, sharey=True, squeeze=False
        )

        for i, topics in enumerate(topics_vals):
            for j, sa in enumerate(sa_vals):
                ax = axes[i][j]
                sub = dfk[(dfk["topics"] == topics) & (dfk["sa"] == sa)].sort_values("mr")
                if sub.empty:
                    ax.set_axis_off()
                    continue

                ax.plot(sub["mr"], sub[seed_col], marker="o", linewidth=2.5, markersize=6, label="Seeding")
                ax.fill_between(sub["mr"], sub[seed_q25], sub[seed_q75], alpha=0.2)

                ax.plot(sub["mr"], sub[das_col], marker="o", linestyle="--", linewidth=2.0, markersize=6, label="DAS")
                ax.fill_between(sub["mr"], sub[das_q25], sub[das_q75], alpha=0.2)

                ax.set_title(f"TOPICS={topics} | SA={sa}")
                ax.set_ylim(-0.02, 1.05)  # headroom so lines at 1.0 are visible
                ax.grid(True, alpha=0.3)

                if i == 0 and j == 0:
                    ax.legend()

                if i == len(topics_vals) - 1:
                    ax.set_xlabel("Malicious Rate (MR)")
                if j == 0:
                    ax.set_ylabel("Success rate (eligible nodes only)")

        fig.suptitle(f"Success vs MR (median ± IQR over seeds) | K={k} | metric={metric}", y=1.02)
        fig.tight_layout()
        out_png = os.path.join(out_dir, f"lines_success_vs_mr_K_{k}_{metric}.png")
        fig.savefig(out_png, dpi=200)
        plt.close(fig)
        print("Saved:", out_png)


def _heatmap(ax, pivot, title, vmin=0.0, vmax=1.0, annotate=True):
    if pivot.empty:
        ax.set_axis_off()
        return None

    im = ax.imshow(pivot.values, origin="lower", aspect="auto", vmin=vmin, vmax=vmax)
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


def plot_heatmaps(agg_df: pd.DataFrame, out_dir: str, metric: str):
    ensure_dir(out_dir)
    topics_vals = sorted(agg_df["topics"].unique().tolist())
    k_vals = sorted(agg_df["k"].unique().tolist())

    seed_col = f"seeding_success_{metric}_median"
    das_col = f"das_success_{metric}_median"

    for topics in topics_vals:
        for k in k_vals:
            sub = agg_df[(agg_df["topics"] == topics) & (agg_df["k"] == k)].copy()
            if sub.empty:
                continue

            se = sub.pivot_table(index="sa", columns="mr", values=seed_col, aggfunc="mean").sort_index().sort_index(axis=1)
            da = sub.pivot_table(index="sa", columns="mr", values=das_col, aggfunc="mean").sort_index().sort_index(axis=1)
            de = (da - se)

            fig, axes = plt.subplots(1, 3, figsize=(15, 4.8))
            im1 = _heatmap(axes[0], se, f"Seeding success ({metric}) | TOPICS={topics} K={k}", vmin=0, vmax=1, annotate=True)
            im2 = _heatmap(axes[1], da, f"DAS success ({metric}) | TOPICS={topics} K={k}", vmin=0, vmax=1, annotate=True)

            if not de.empty:
                vmax = float(np.nanmax(np.abs(de.values)))
                vmax = vmax if np.isfinite(vmax) and vmax > 0 else 1e-6
                im3 = _heatmap(axes[2], de, f"Delta (DAS-Seeding) | TOPICS={topics} K={k}",
                               vmin=-vmax, vmax=vmax, annotate=True)
            else:
                axes[2].set_axis_off()
                im3 = None

            if im1 is not None:
                fig.colorbar(im1, ax=axes[0], fraction=0.04, pad=0.02)
            if im2 is not None:
                fig.colorbar(im2, ax=axes[1], fraction=0.04, pad=0.02)
            if im3 is not None:
                fig.colorbar(im3, ax=axes[2], fraction=0.04, pad=0.02)

            fig.suptitle(f"Success heatmaps (median over seeds) | metric={metric} | eligible nodes only", y=1.03)
            fig.tight_layout()
            out_png = os.path.join(out_dir, f"heatmaps_TOPICS_{topics}_K_{k}_{metric}.png")
            fig.savefig(out_png, dpi=200)
            plt.close(fig)
            print("Saved:", out_png)


def plot_tradeoff_html(agg_df: pd.DataFrame, out_html: str, metric: str):
    try:
        import plotly.express as px
    except Exception:
        print("Plotly not installed -> skipping HTML. Install: pip install plotly")
        return

    seed_col = f"seeding_success_{metric}_median"
    das_col = f"das_success_{metric}_median"

    df = agg_df.copy()
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[seed_col, das_col, "mr"])

    fig = px.scatter(
        df,
        x=seed_col,
        y=das_col,
        color="mr",
        symbol="sa",
        size="k",
        hover_data=[
            "topics", "k", "sa", "mr",
            seed_col, das_col,
            "excluded_fraction_final_median",
            "seeding_target_median",
        ],
        title=f"Tradeoff: Seeding vs DAS (median over seeds) | metric={metric} | eligible nodes only"
    )
    fig.update_xaxes(range=[0, 1], title="Seeding success (median, eligible only)")
    fig.update_yaxes(range=[0, 1], title="DAS success (median, eligible only)")
    fig.write_html(out_html)
    print("Saved:", out_html)


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root folder containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("--out", default="success_out", help="Output directory")
    ap.add_argument("--das-target", type=int, default=73, help="DAS target: Total Sample Received needed for full success")
    ap.add_argument("--log-every", type=int, default=25, help="Progress print every N files")
    args = ap.parse_args()

    ensure_dir(args.out)

    idx = build_file_index(args.root)
    print("Indexed files:", len(idx))
    print(idx.head(3))

    rows = []
    total = len(idx)

    for i, r in enumerate(idx.itertuples(index=False), start=1):
        if i == 1 or i % args.log_every == 0 or i == total:
            print(f"[{i:>4}/{total}] {r.path}")

        # NOTE: seeding now depends on SA (not K)
        s = summarize_success_one_csv(
            path=r.path,
            sa=int(r.sa),
            das_target=int(args.das_target)
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
    seed_csv = os.path.join(args.out, "summary_per_seed.csv")
    seed_df.to_csv(seed_csv, index=False)
    print("Saved:", seed_csv)

    agg_df = aggregate_median_iqr(seed_df)
    agg_csv = os.path.join(args.out, "summary_median_iqr.csv")
    agg_df.to_csv(agg_csv, index=False)
    print("Saved:", agg_csv)

    for metric in ["mean", "strict"]:
        plot_lines_success_vs_mr(agg_df, out_dir=os.path.join(args.out, f"lines_per_k_{metric}"), metric=metric)
        plot_heatmaps(agg_df, out_dir=os.path.join(args.out, f"heatmaps_{metric}"), metric=metric)
        plot_tradeoff_html(agg_df, out_html=os.path.join(args.out, f"tradeoff_{metric}.html"), metric=metric)

    print("\nDone. Outputs in:", args.out)
    print("Notes:")
    print(" - Eligible nodes only: Total Sample Req Sent > 0")
    print(" - Seeding target per SA: ceil(SA/2)")
    print(" - DAS target:", args.das_target)


if __name__ == "__main__":
    main()

