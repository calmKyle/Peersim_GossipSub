import os
import re
import math
import ast
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time

# ============================================================
# CONFIGURATION: BASELINE DEFAULTS
# ============================================================
ROOT_DIR = "."  

# When we vary one parameter (e.g. K), we need to know what to 
# fix the OTHERS to. These are your "Baseline" settings.
DEFAULT_CSVOUT = "CSVOut"
DEFAULT_TOPICS = 128
DEFAULT_K      = 1
DEFAULT_MR     = 0.3
DEFAULT_SA     = 1

# ============================================================
# 1) Indexing & Loading
# ============================================================
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

def build_file_index(root_dir: str) -> pd.DataFrame:
    rows = []
    print(f"Scanning directory: {root_dir} ...")
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
                "csvout": csvout, "topics": topics, "k": k, 
                "mr": mr, "sa": sa, "seed": seed,
                "path": os.path.join(dirpath, fn),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"No matching CSVs found under: {root_dir}")
    return df.sort_values(["csvout","topics","k","mr","sa","seed"]).reset_index(drop=True)

def parse_times_cell(x):
    if pd.isna(x): return []
    if isinstance(x, (int, float)): return [float(x)] if np.isfinite(x) else []
    s = str(x).strip()
    if s in ("", "[]", "nan", "None"): return []
    if s.startswith("[") and s.endswith("]"):
        try:
            return [float(i) for i in s[1:-1].split(",") if i.strip()]
        except: pass
    out = []
    for part in s.replace(";", ",").split(","):
        try:
            fv = float(part)
            if np.isfinite(fv): out.append(fv)
        except: pass
    return out

def flatten_times_for_block(df_block, col, max_points=None):
    acc = []
    for x in df_block[col].values:
        acc.extend(parse_times_cell(x))
    arr = np.asarray(acc, dtype=float)
    arr = arr[np.isfinite(arr)]
    if max_points is not None and len(arr) > max_points:
        rng = np.random.default_rng(0)
        idx = rng.choice(len(arr), size=max_points, replace=False)
        arr = arr[idx]
    return arr

# ============================================================
# 2) Plotting Logic
# ============================================================
def plot_cdf_overlay(index_subset, vary_param, arrival_col, title):
    fig, ax = plt.subplots(figsize=(10, 6))
    probs = np.linspace(0.0, 1.0, 100)
    groups = index_subset.groupby(vary_param)
    
    found_data = False
    for param_val, sub_df in groups:
        paths = sub_df["path"].tolist()
        per_seed_quantiles = []
        
        for pth in paths:
            d = pd.read_csv(pth, usecols=["Time", arrival_col])
            times = sorted(d["Time"].unique())
            all_latencies = []
            for i, t in enumerate(times):
                start_t = 0 if i == 0 else times[i-1]
                block_data = d[d["Time"] == t]
                arr = flatten_times_for_block(block_data, arrival_col, max_points=50000)
                arr = arr - start_t
                arr = arr[arr >= 0]
                all_latencies.extend(arr)

            if not all_latencies: continue
            arr_np = np.array(all_latencies)
            arr_np.sort()
            if len(arr_np) > 0:
                q = np.quantile(arr_np, probs)
                per_seed_quantiles.append(q)
        
        if per_seed_quantiles:
            found_data = True
            Q = np.vstack(per_seed_quantiles)
            q_med = np.median(Q, axis=0)
            ax.plot(q_med, probs, label=f"{vary_param.upper()}={param_val}", linewidth=2)

    ax.set_title(title)
    ax.set_xlabel("Latency (Time since block start)")
    ax.set_ylabel("CDF (Probability)")
    ax.grid(True, linestyle="--", alpha=0.6)
    if found_data:
        ax.legend(title=vary_param.upper())
    return fig

def plot_bar_overlay(index_subset, vary_param, title):
    groups = index_subset.groupby(vary_param)
    labels, means, stds = [], [], []
    
    for param_val, sub_df in groups:
        paths = sub_df["path"].tolist()
        seed_means = []
        for p in paths:
            d = pd.read_csv(p, usecols=["Duplicated_Data", "Shards_Amount"])
            d["dup"] = pd.to_numeric(d["Duplicated_Data"], errors="coerce")
            d["shards"] = pd.to_numeric(d["Shards_Amount"], errors="coerce")
            d["ratio"] = d["dup"] / d["shards"]
            seed_means.append(d["ratio"].mean())
            
        if seed_means:
            labels.append(str(param_val))
            means.append(np.mean(seed_means))
            stds.append(np.std(seed_means))

    fig, ax = plt.subplots(figsize=(8, 5))
    x_pos = np.arange(len(labels))
    ax.bar(x_pos, means, yerr=stds, capsize=5, alpha=0.7)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels)
    ax.set_xlabel(vary_param.upper())
    ax.set_ylabel("Avg Duplication / Shard")
    ax.set_title(title)
    return fig

# ============================================================
# 3) BATCH RUNNER LOGIC
# ============================================================
def run_analysis_for_param(vary_param, idx):
    """
    Filters data fixing everything EXCEPT 'vary_param', 
    then generates the 3 standard plots.
    """
    print(f"\n" + "="*60)
    print(f" ANALYSIS: VARYING '{vary_param.upper()}'")
    print(f"="*60)

    # 1. Build Mask based on DEFAULTs
    mask = (idx["csvout"] == DEFAULT_CSVOUT)

    if vary_param != "topics":
        mask &= (idx["topics"] == DEFAULT_TOPICS)
    if vary_param != "k":
        mask &= (idx["k"] == DEFAULT_K)
    if vary_param != "mr":
        mask &= np.isclose(idx["mr"], DEFAULT_MR)
    if vary_param != "sa":
        mask &= (idx["sa"] == DEFAULT_SA)

    subset = idx[mask].copy()
    unique_vals = sorted(subset[vary_param].unique())

    # 2. Check Data Availability
    if len(subset) == 0:
        print(f"[SKIP] No data found for varying {vary_param.upper()} with current defaults.")
        print(f"       Expected Context: Topics={DEFAULT_TOPICS}, K={DEFAULT_K}, MR={DEFAULT_MR}, SA={DEFAULT_SA}")
        return

    print(f"Context: Fixed others to Defaults (Topics={DEFAULT_TOPICS}, K={DEFAULT_K}...)")
    print(f"Comparing {vary_param.upper()} values: {unique_vals}")
    print(f"Files found: {len(subset)}")

    # 3. Generate Plots
    print(f"Generating plots for {vary_param.upper()}...")
    
    # Plot 1: Sample Arrival
    fig1 = plot_cdf_overlay(
        subset, vary_param, 
        arrival_col="Sample Arrival Times", 
        title=f"Sample Arrival Time (Varying {vary_param.upper()})"
    )
    plt.show()

    # Plot 2: Seed Arrival
    fig2 = plot_cdf_overlay(
        subset, vary_param, 
        arrival_col="Seed Part Arrival Times", 
        title=f"Seed Part Arrival Time (Varying {vary_param.upper()})"
    )
    plt.show()

    # Plot 3: Bandwidth
    fig3 = plot_bar_overlay(
        subset, vary_param, 
        title=f"Bandwidth Overhead (Varying {vary_param.upper()})"
    )
    plt.show()

# ============================================================
# 4) MAIN EXECUTION
# ============================================================
if __name__ == "__main__":
    idx = build_file_index(ROOT_DIR)
    print(f"Total files indexed: {len(idx)}")

    # LIST OF PARAMETERS TO COMPARE
    params_to_compare = ["topics", "k", "sa", "mr"]

    for param in params_to_compare:
        run_analysis_for_param(param, idx)
        
    print("\nAll comparisons completed.")
