#!/usr/bin/env python3
import os
import re
import time
import argparse
import numpy as np
import pandas as pd

# ----------------------------
# File pattern
# ----------------------------
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

# ----------------------------
# Utilities
# ----------------------------
def fmt_secs(s: float) -> str:
    if not np.isfinite(s):
        return "inf"
    s = int(max(0, s))
    return f"{s//60}m{s%60:02d}s" if s >= 60 else f"{s}s"

def safe_numeric(series):
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)

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

def summarize_one_csv_final(path: str) -> dict:
    """
    Fast per-seed summary using ONLY final snapshot.
    Metrics:
      - dup_per_shard_mean_final
      - dup_per_shard_median_final
      - bandwidth_mean_final
      - dup_data_mean_final
      - dup_ihave_mean_final
      - sample_rtt_median_final
      - seed_part_rtt_median_final

    Note: RTT columns are taken "as-is" here. If you want "since slot start",
    use the slot normalization you already implemented in your other scripts.
    """
    # pass 1: final time
    tcol = pd.read_csv(path, usecols=["Time"], low_memory=False)
    times = safe_numeric(tcol["Time"]).dropna()
    if len(times) == 0:
        return {
            "final_time": np.nan,
            "dup_per_shard_mean_final": np.nan,
            "dup_per_shard_median_final": np.nan,
            "bandwidth_mean_final": np.nan,
            "dup_data_mean_final": np.nan,
            "dup_ihave_mean_final": np.nan,
            "sample_rtt_median_final": np.nan,
            "seed_part_rtt_median_final": np.nan,
        }
    final_time = int(times.max())

    usecols = [
        "Time",
        "Duplicated_Data", "Shards_Amount",
        "Avg Bandwidth",
        "Duplicated_IHAVE",
        "Avg Sample RTT",
        "Avg Seed Part RTT",
    ]
    d = pd.read_csv(path, usecols=lambda c: c in usecols, low_memory=False)
    d = d[safe_numeric(d["Time"]) == final_time].copy()
    if d.empty:
        return {
            "final_time": final_time,
            "dup_per_shard_mean_final": np.nan,
            "dup_per_shard_median_final": np.nan,
            "bandwidth_mean_final": np.nan,
            "dup_data_mean_final": np.nan,
            "dup_ihave_mean_final": np.nan,
            "sample_rtt_median_final": np.nan,
            "seed_part_rtt_median_final": np.nan,
        }

    out = {"final_time": final_time}

    # duplication per shard
    if "Duplicated_Data" in d.columns and "Shards_Amount" in d.columns:
        dup = safe_numeric(d["Duplicated_Data"])
        shards = safe_numeric(d["Shards_Amount"])
        x = (dup / shards).replace([np.inf, -np.inf], np.nan).dropna()
        out["dup_per_shard_mean_final"] = float(x.mean()) if len(x) else np.nan
        out["dup_per_shard_median_final"] = float(x.median()) if len(x) else np.nan
        out["dup_data_mean_final"] = float(dup.dropna().mean()) if dup.notna().any() else np.nan
    else:
        out["dup_per_shard_mean_final"] = np.nan
        out["dup_per_shard_median_final"] = np.nan
        out["dup_data_mean_final"] = np.nan

    # bandwidth mean
    if "Avg Bandwidth" in d.columns:
        bw = safe_numeric(d["Avg Bandwidth"]).dropna()
        out["bandwidth_mean_final"] = float(bw.mean()) if len(bw) else np.nan
    else:
        out["bandwidth_mean_final"] = np.nan

    # duplicated ihave
    if "Duplicated_IHAVE" in d.columns:
        ih = safe_numeric(d["Duplicated_IHAVE"]).dropna()
        out["dup_ihave_mean_final"] = float(ih.mean()) if len(ih) else np.nan
    else:
        out["dup_ihave_mean_final"] = np.nan

    # RTT medians (as stored)
    if "Avg Sample RTT" in d.columns:
        srt = safe_numeric(d["Avg Sample RTT"]).dropna()
        out["sample_rtt_median_final"] = float(srt.median()) if len(srt) else np.nan
    else:
        out["sample_rtt_median_final"] = np.nan

    if "Avg Seed Part RTT" in d.columns:
        spt = safe_numeric(d["Avg Seed Part RTT"]).dropna()
        out["seed_part_rtt_median_final"] = float(spt.median()) if len(spt) else np.nan
    else:
        out["seed_part_rtt_median_final"] = np.nan

    return out

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
# 3D plot (Plotly) + fallback
# ----------------------------
def make_3d_plot(cfg_df: pd.DataFrame, out_html: str,
                x="sa", y="k", z="topics",
                color="dup_per_shard_mean_final",
                size=None,
                split_by="mr",
                title=None):
    """
    Interactive 3D scatter:
      x,y,z are axes
      color is metric (good/bad)
      size optional
      split_by: create dropdown filter (default mr)
    """
    try:
        import plotly.express as px
        import plotly.graph_objects as go
    except Exception:
        raise RuntimeError("Plotly is not installed. Install with: pip install plotly")

    df = cfg_df.copy()
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[x,y,z,color])

    if title is None:
        title = f"3D compare: x={x}, y={y}, z={z}, color={color}" + (f", size={size}" if size else "")

    if split_by and split_by in df.columns and df[split_by].nunique() > 1:
        # build one trace per split value + dropdown
        traces = []
        buttons = []
        split_vals = sorted(df[split_by].unique().tolist())

        # color scale range fixed across traces
        cmin = float(df[color].min())
        cmax = float(df[color].max())

        for i, sv in enumerate(split_vals):
            sub = df[df[split_by] == sv]

            marker = dict(
                color=sub[color],
                colorscale="Viridis",
                cmin=cmin, cmax=cmax,
                colorbar=dict(title=color) if i == 0 else None,
                opacity=0.85,
                size=(sub[size] if size and size in sub.columns else 6),
                sizemode="diameter",
            )

            trace = go.Scatter3d(
                x=sub[x], y=sub[y], z=sub[z],
                mode="markers",
                marker=marker,
                name=f"{split_by}={sv}",
                text=[
                    f"{split_by}={sv}<br>MR={r.mr} SA={r.sa} K={r.k} TOPICS={r.topics}<br>{color}={r.__getattribute__(color):.4g}"
                    for r in sub.itertuples(index=False)
                ],
                hoverinfo="text",
                visible=(i == 0)
            )
            traces.append(trace)

            vis = [False]*len(split_vals)
            vis[i] = True
            buttons.append(dict(
                label=f"{split_by}={sv}",
                method="update",
                args=[{"visible": vis}, {"title": title + f" | {split_by}={sv}"}]
            ))

        fig = go.Figure(data=traces)
        fig.update_layout(
            title=title + f" | {split_by}={split_vals[0]}",
            scene=dict(
                xaxis_title=x,
                yaxis_title=y,
                zaxis_title=z
            ),
            updatemenus=[dict(buttons=buttons, direction="down", x=1.02, y=1.0)]
        )
        fig.write_html(out_html)
    else:
        # single plot
        fig = px.scatter_3d(
            df, x=x, y=y, z=z,
            color=color,
            size=size if size and size in df.columns else None,
            hover_data=["mr","sa","k","topics",color],
            title=title,
            color_continuous_scale="Viridis"
        )
        fig.write_html(out_html)

    print("Saved:", out_html)

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Root folder containing CSVOut*/TOPICS_AMOUNT_*/K_*/...")
    ap.add_argument("--out", default="merged_3d", help="Output folder")
    ap.add_argument("--summary", default="", help="Optional: existing median-over-seeds summary CSV to reuse")
    ap.add_argument("--log-every", type=int, default=10, help="Print progress every N files while summarizing")
    ap.add_argument("--x", default="sa", help="3D x axis column (default sa)")
    ap.add_argument("--y", default="k", help="3D y axis column (default k)")
    ap.add_argument("--z", default="topics", help="3D z axis column (default topics)")
    ap.add_argument("--color", default="dup_per_shard_mean_final", help="Color metric column")
    ap.add_argument("--size", default="", help="Optional size column (e.g., bandwidth_mean_final)")
    ap.add_argument("--split-by", default="mr", help="Dropdown split column (default mr, set '' to disable)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # If user already has a summary, use it
    if args.summary and os.path.exists(args.summary):
        cfg_df = pd.read_csv(args.summary)
        print("Loaded summary:", args.summary, "| rows:", len(cfg_df))
    else:
        idx = build_file_index(args.root)
        print("Indexed files:", len(idx))
        print(idx.head())

        # per-seed summaries
        t0 = time.time()
        rows = []
        total = len(idx)
        print(f"\nSummarizing {total} files (final snapshot only)...")

        for i, r in enumerate(idx.itertuples(index=False), start=1):
            if i == 1 or i % args.log_every == 0 or i == total:
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta = (total - i) / rate if rate > 0 else float("inf")
                print(f"[{i:>4}/{total}] elapsed={fmt_secs(elapsed)} rate={rate:.2f} files/s ETA={fmt_secs(eta)}")
                print(f"   -> TOPICS={r.topics} K={r.k} MR={r.mr} SA={r.sa} seed={r.seed}")

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
            rows.append(s)

        seed_df = pd.DataFrame(rows)
        seed_csv = os.path.join(args.out, "summary_per_seed_final.csv")
        seed_df.to_csv(seed_csv, index=False)
        print("Saved:", seed_csv)

        cfg_df = aggregate_median_over_seeds(seed_df)
        cfg_csv = os.path.join(args.out, "config_summary_median_over_seeds.csv")
        cfg_df.to_csv(cfg_csv, index=False)
        print("Saved:", cfg_csv)

    # Show available columns for plotting
    print("\nColumns available for 3D axes/color/size:")
    print(list(cfg_df.columns))

    # Make the 3D plot
    out_html = os.path.join(args.out, f"3d_x-{args.x}_y-{args.y}_z-{args.z}_c-{args.color}.html")
    size_col = args.size if args.size else None
    split_by = args.split_by if args.split_by else None

    make_3d_plot(
        cfg_df=cfg_df,
        out_html=out_html,
        x=args.x,
        y=args.y,
        z=args.z,
        color=args.color,
        size=size_col,
        split_by=split_by,
        title=None
    )

    # Also write a ranked table by the color metric (useful to see best/worst)
    ranked = cfg_df.sort_values(args.color, ascending=True).reset_index(drop=True)
    ranked_csv = os.path.join(args.out, f"ranked_by_{args.color}.csv")
    ranked.to_csv(ranked_csv, index=False)
    print("Saved:", ranked_csv)
    print("\nTip: If lower is better (duplication/latency), the top rows are best.")


if __name__ == "__main__":
    main()

