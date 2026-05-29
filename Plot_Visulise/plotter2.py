import os
import re
import math
import ast
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# 0) CONFIG
# ============================================================
ROOT_DIR = "./" 

# ============================================================
# 1) Indexing & Parsing (Same as before)
# ============================================================
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

def build_file_index(root_dir: str) -> pd.DataFrame:
    rows = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if not fn.endswith(".csv"):
                continue
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
    return df.sort_values(["csvout", "topics", "k", "mr", "sa", "seed"]).reset_index(drop=True)

def parse_times_cell(x):
    if pd.isna(x): return []
    if isinstance(x, (int, float)): return [float(x)] if np.isfinite(x) else []
    s = str(x).strip()
    if s in ("", "[]", "nan", "None"): return []
    # Fast path for simple brackets
    if s.startswith("[") and s.endswith("]"):
        try:
            return [float(i) for i in s[1:-1].split(",") if i.strip()]
        except:
            pass
    # Fallback
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
# 2) Comparison Plotting Functions
# ============================================================

def plot_comparison_cdf(
    config_groups: dict,  # { "Label": [seed_paths...], ... }
    arrival_col: str,
    title: str,
    probs_points: int = 100,
    max_points: int = 100_000
):
    """
    Plots one CDF line per configuration group (median over seeds).
    Aggregates ALL blocks together to give a general performance overview.
    """
    probs = np.linspace(0.0, 1.0, probs_points)
    fig, ax = plt.subplots(figsize=(10, 6))

    for label, paths in config_groups.items():
        if not paths:
            continue
        
        # Collect quantiles per seed (aggregated over all blocks for simplicity)
        per_seed_quantiles = []
        
        for pth in paths:
            d = pd.read_csv(pth, usecols=["Time", arrival_col])
            # Flatten ALL blocks in this seed (relative to block start if needed, 
            # but usually for comparison raw time or simple aggregation is okay. 
            # Here we reset time per block to 0 to see 'latency')
            
            all_latency = []
            # Group by time to subtract start time
            times = sorted(d["Time"].unique())
            for i, t in enumerate(times):
                start_t = 0 if i == 0 else times[i-1]
                block_data = d[d["Time"] == t]
                arr = flatten_times_for_block(block_data, arrival_col, max_points=max_points)
                # Calculate latency (Time since block start)
                arr = arr - start_t
                arr = arr[arr >= 0]
                all_latency.extend(arr)
            
            if not all_latency:
                continue

            arr_np = np.array(all_latency)
            if len(arr_np) > max_points:
                rng = np.random.default_rng(0)
                arr_np = rng.choice(arr_np, size=max_points, replace=False)
            
            arr_np.sort()
            q = np.quantile(arr_np, probs)
            per_seed_quantiles.append(q)

        if per_seed_quantiles:
            # Median quantile across seeds
            Q = np.vstack(per_seed_quantiles)
            q_med = np.median(Q, axis=0)
            ax.plot(q_med, probs, label=label, linewidth=2)

    ax.set_title(title)
    ax.set_xlabel("Latency (Time since block start)")
    ax.set_ylabel("CDF (Probability)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend()
    return fig

def plot_comparison_bar(
    config_groups: dict,
    title: str
):
    """
    Bar chart comparing Average Duplication per Shard per Node.
    """
    labels = []
    means = []
    errors = []

    for label, paths in config_groups.items():
        if not paths: continue
        
        seed_means = []
        for p in paths:
            d = pd.read_csv(p, usecols=["Duplicated_Data", "Shards_Amount"])
            d["dup"] = pd.to_numeric(d["Duplicated_Data"], errors="coerce")
            d["shards"] = pd.to_numeric(d["Shards_Amount"], errors="coerce")
            d["ratio"] = d["dup"] / d["shards"]
            # Mean for this seed
            seed_means.append(d["ratio"].mean())
        
        if seed_means:
            labels.append(label)
            means.append(np.mean(seed_means))
            # Standard deviation across seeds
            errors.append(np.std(seed_means))

    fig, ax = plt.subplots(figsize=(8, 5))
    x_pos = np.arange(len(labels))
    ax.bar(x_pos, means, yerr=errors, capsize=5, alpha=0.7)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Avg Duplication / Shard")
    ax.set_title(title)
    fig.tight_layout()
    return fig


# ============================================================
# 3) MAIN - COMPARISON LOGIC
# ============================================================
if __name__ == "__main__":
    idx = build_file_index(ROOT_DIR)
    
    # --------------------------------------------------------
    # CONFIGURATION: What do you want to compare?
    # --------------------------------------------------------
    
    # 1. FIXED PARAMETERS (Select the context)
    #    (Update these to match your actual folder structure/available data)
    FIXED_CSVOUT = "CSVOut"  # e.g. "CSVOut", "CSVOut_0_0"
    FIXED_TOPICS = 128
    FIXED_K      = 1
    
    # 2. VARIABLE PARAMETER (What changes?)
    #    Choose one: "sa", "mr", "k", "topics"
    VARY_PARAM   = "sa" 
    
    # 3. OTHER FIXED (The params you are NOT varying)
    #    If you vary 'sa', you must fix 'mr'. 
    #    If you vary 'mr', you must fix 'sa'.
    FIXED_MR = 0.3
    FIXED_SA = 1  # Ignored if VARY_PARAM is 'sa'

    # --------------------------------------------------------
    # Execution
    # --------------------------------------------------------
    print(f"--- Comparing effect of '{VARY_PARAM.upper()}' ---")
    
    # Filter for fixed context first
    mask = (idx["csvout"] == FIXED_CSVOUT) & \
           (idx["topics"] == FIXED_TOPICS) & \
           (idx["k"] == FIXED_K)
    
    subset = idx[mask].copy()

    # Further filter based on what is NOT varying
    if VARY_PARAM != "mr":
        subset = subset[np.isclose(subset["mr"], FIXED_MR)]
    if VARY_PARAM != "sa":
        subset = subset[subset["sa"] == FIXED_SA]
        
    # Get unique values of the varying parameter
    unique_vals = sorted(subset[VARY_PARAM].unique())
    print(f"Found values for {VARY_PARAM}: {unique_vals}")

    # Build the dictionary: { "SA=1": [paths...], "SA=4": [paths...] }
    config_groups = {}
    for val in unique_vals:
        paths = subset[subset[VARY_PARAM] == val]["path"].tolist()
        label = f"{VARY_PARAM.upper()}={val}"
        config_groups[label] = paths
        print(f"  {label}: {len(paths)} seeds")

    if not config_groups:
        print("No matching files found. Check your FIXED parameters.")
        exit()

    # --------------------------------------------------------
    # PLOTS
    # --------------------------------------------------------

    # 1. Compare Sample Arrival Times (CDF)
    print("Plotting Sample Arrival CDF...")
    fig_cdf = plot_comparison_cdf(
        config_groups, 
        arrival_col="Sample Arrival Times", 
        title=f"Impact of {VARY_PARAM.upper()} on Sample Arrival Time",
        max_points=50000
    )
    plt.show()

    # 2. Compare Seed Part Arrival Times (CDF)
    print("Plotting Seed Part Arrival CDF...")
    fig_seed = plot_comparison_cdf(
        config_groups, 
        arrival_col="Seed Part Arrival Times", 
        title=f"Impact of {VARY_PARAM.upper()} on Seed Part Arrival",
        max_points=50000
    )
    plt.show()

    # 3. Compare Duplication (Bar Chart)
    print("Plotting Duplication Comparison...")
    fig_bar = plot_comparison_bar(
        config_groups,
        title=f"Impact of {VARY_PARAM.upper()} on Bandwidth (Duplication)"
    )
    plt.show()
