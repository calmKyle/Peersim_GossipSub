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
ROOT_DIR = "."  # if you run from the folder that contains CSVOut / CSVOut_0_0 etc.


# ============================================================
# 1) Index all CSV files
# ============================================================
FILENAME_RE = re.compile(
    r"output_results_MR_(?P<mr>[\d.]+)_SA_(?P<sa>\d+)_seed_(?P<seed>\d+)\.0\.csv$"
)

def build_file_index(root_dir: str) -> pd.DataFrame:
    rows = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            m = FILENAME_RE.match(fn)
            if not m:
                continue

            parts = dirpath.replace("\\", "/").split("/")

            # ✅ Instead of only CSVOut_0_0, grab the first folder that starts with CSVOut
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

    return df.sort_values(["csvout", "topics", "k", "mr", "sa", "seed"]).reset_index(drop=True)


# ============================================================
# 2) Parsing helper (arrival time cells)
# ============================================================
def parse_times_cell(x):
    if pd.isna(x):
        return []

    if isinstance(x, (int, float, np.integer, np.floating)):
        return [float(x)] if np.isfinite(x) else []

    s = str(x).strip()
    if s in ("", "[]", "nan", "None"):
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            v = ast.literal_eval(s)
            if isinstance(v, (list, tuple)):
                out = []
                for item in v:
                    try:
                        fv = float(item)
                        if np.isfinite(fv):
                            out.append(fv)
                    except Exception:
                        pass
                return out
        except Exception:
            s = s[1:-1].strip()
            if not s:
                return []

    out = []
    for part in s.split(";"):
        part = part.strip()
        if not part:
            continue
        for q in part.split(","):
            q = q.strip()
            if not q:
                continue
            try:
                fv = float(q)
                if np.isfinite(fv):
                    out.append(fv)
            except Exception:
                pass
    return out


def flatten_times_for_block(df_block: pd.DataFrame, col: str, max_points=None) -> np.ndarray:
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


def get_block_times(df: pd.DataFrame) -> list[int]:
    times = pd.to_numeric(df["Time"], errors="coerce").dropna().unique()
    return sorted(times.astype(int).tolist())


def block_start_time(times: list[int], block_index: int) -> int:
    return 0 if block_index == 0 else times[block_index - 1]


# ============================================================
# 3) Selecting configs that actually exist
# ============================================================
def show_available(idx: pd.DataFrame):
    print("\nAvailable CSVOut roots:", sorted(idx["csvout"].unique().tolist()))
    print("Available TOPICS:", sorted(idx["topics"].unique().tolist()))
    print("Available K:", sorted(idx["k"].unique().tolist()))
    print("Available MR:", sorted(idx["mr"].unique().tolist()))
    print("Available SA:", sorted(idx["sa"].unique().tolist()))


def pick_config_paths(idx: pd.DataFrame, csvout: str, topics: int, k: int, mr: float, sa: int) -> list[str]:
    sel = idx[
        (idx["csvout"] == csvout) &
        (idx["topics"] == topics) &
        (idx["k"] == k) &
        (np.isclose(idx["mr"], mr)) &
        (idx["sa"] == sa)
    ].sort_values("seed")

    if sel.empty:
        raise RuntimeError(
            "No files match that configuration.\n"
            f"Try one that exists. For example:\n"
            f"{idx[['csvout','topics','k','mr','sa']].drop_duplicates().head(10)}"
        )
    return sel["path"].tolist()


# ============================================================
# 4) Median over seeds for duplication per shard
# ============================================================
def load_dup_per_shard_median_over_seeds(seed_paths: list[str]) -> pd.DataFrame:
    keep_cols = ["Time", "Node ID", "Duplicated_Data", "Shards_Amount"]
    frames = []

    for p in seed_paths:
        d = pd.read_csv(p, usecols=lambda c: c in keep_cols)
        d["Duplicated_Data"] = pd.to_numeric(d["Duplicated_Data"], errors="coerce")
        d["Shards_Amount"] = pd.to_numeric(d["Shards_Amount"], errors="coerce")
        d["dup_per_shard"] = d["Duplicated_Data"] / d["Shards_Amount"]
        frames.append(d[["Time", "Node ID", "dup_per_shard"]])

    all_seeds = pd.concat(frames, ignore_index=True)
    med = (
        all_seeds
        .groupby(["Time", "Node ID"], as_index=False)["dup_per_shard"]
        .median()
        .sort_values(["Time", "Node ID"])
        .reset_index(drop=True)
    )
    return med


# ============================================================
# 5) Plots
# ============================================================
def plot_duplication_per_shard_per_block(med_dup: pd.DataFrame, bins: int = 40):
    times = sorted(med_dup["Time"].unique().astype(int).tolist())
    print("\n=== BLOCKS / SNAPSHOTS ===")
    print("Time snapshots:", times)
    print("Block mapping:")
    for i, t in enumerate(times):
        start = 0 if i == 0 else times[i-1]
        print(f"  Block {i} -> snapshot Time={t}, block_start={start}")
    
    cols = 3
    rows = math.ceil(len(times) / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(12, 6), sharex=True, sharey=True)
    axes = np.array(axes).reshape(-1)

    for i, t in enumerate(times):
        ax = axes[i]
        vals = med_dup.loc[med_dup["Time"] == t, "dup_per_shard"].dropna().to_numpy()
        ax.hist(vals, bins=bins)
        ax.set_title(f"Block {i+1}")
        if i % cols == 0:
            ax.set_ylabel("Nodes")
        if i >= (rows - 1) * cols:
            ax.set_xlabel("Dup per shard")

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.suptitle("Duplication per shard distribution per block (median over seeds)")
    fig.tight_layout()
    return fig


def plot_avg_dup_per_shard_per_slot(med_dup: pd.DataFrame):
    means = med_dup.groupby("Time")["dup_per_shard"].mean()
    times = sorted(means.index.astype(int).tolist())
    y = means.reindex(times).to_numpy()

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar([str(t) for t in times], y)
    ax.set_title("Average duplicated data per shard per slot (median over seeds)")
    ax.set_xlabel("Slot start time")
    ax.set_ylabel("Avg duplicated data per shard")
    fig.tight_layout()
    return fig


def plot_violin_avg_dup_per_shard_per_node(med_dup: pd.DataFrame):
    node_avg = med_dup.groupby("Node ID")["dup_per_shard"].mean().dropna().to_numpy()
    med = float(np.median(node_avg))

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.violinplot([node_avg], vert=False, showmeans=False, showextrema=False)
    ax.scatter([med], [1], marker="D", label="Median")
    ax.set_yticks([])
    ax.set_title("Distribution of avg duplicated data per shard (all slots, median over seeds)")
    ax.set_xlabel("Avg duplicated data per shard (per node)")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_cdf_arrival_times_per_block_median_over_seeds(
    seed_paths: list[str],
    arrival_col: str,
    title: str,
    probs_points: int = 200,
    max_points_per_seed_per_block: int | None = 300_000,
):
    probs = np.linspace(0.0, 1.0, probs_points)

    df0 = pd.read_csv(seed_paths[0], usecols=["Time"])
    times = get_block_times(df0)

    cols = 3
    rows = math.ceil(len(times) / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(12, 6), sharex=True, sharey=True)
    axes = np.array(axes).reshape(-1)

    for bi, t in enumerate(times):
        start = block_start_time(times, bi)
        per_seed_quantiles = []

        for pth in seed_paths:
            d = pd.read_csv(pth, usecols=["Time", arrival_col])
            block_df = d[d["Time"] == t]
            arr = flatten_times_for_block(block_df, arrival_col, max_points=max_points_per_seed_per_block)
            arr = arr - start
            arr = arr[np.isfinite(arr)]
            arr = arr[arr >= 0]
            if len(arr) == 0:
                continue
            per_seed_quantiles.append(np.quantile(arr, probs, method="linear"))

        ax = axes[bi]
        ax.set_title(f"Block {bi}")
        if not per_seed_quantiles:
            continue

        q_med = np.median(np.vstack(per_seed_quantiles), axis=0)
        ax.plot(q_med, probs)

        if bi % cols == 0:
            ax.set_ylabel("CDF (0..1)")
        if bi >= (rows - 1) * cols:
            ax.set_xlabel("Arrival time since block start")

    for j in range(bi + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.suptitle(title + " (median over seeds)")
    fig.tight_layout()
    return fig


# ============================================================
# 6) MAIN
# ============================================================
if __name__ == "__main__":
    idx = build_file_index(ROOT_DIR)
    print("Indexed files:", len(idx))
    print("Example rows:\n", idx.head())

    show_available(idx)

    CSVOUT = "CSVOut"     # <-- matches your index
    TOPICS = 128
    K = 1
    MR = 0.3
    SA = 1

    seed_paths = pick_config_paths(idx, CSVOUT, TOPICS, K, MR, SA)
    print("\n=== PLOTTING CONFIG ===")
    print(f"csvout={CSVOUT} | topics={TOPICS} | K={K} | MR={MR} | SA={SA}")
    print("Seed files:")
    for p in seed_paths:
        print(" -", p)

    print("\nSeed files found:", len(seed_paths))
    print("Seeds:", [int(re.search(r"seed_(\d+)", p).group(1)) for p in seed_paths])

    # ---- Duplication plots (median over seeds) ----
    med_dup = load_dup_per_shard_median_over_seeds(seed_paths)

    print("\n[Plot] Duplication per shard distribution per block (hist grid)")
    plot_duplication_per_shard_per_block(med_dup, bins=40)
    plt.show()

    print("\n[Plot] Average duplicated data per shard per slot (bar)")
    plot_avg_dup_per_shard_per_slot(med_dup)
    plt.show()

    print("\n[Plot] Violin: per-node avg duplicated data per shard (all slots)")
    plot_violin_avg_dup_per_shard_per_node(med_dup)
    plt.show()

    print("\n[Plot] CDF per block: Seed Part Arrival Times (median over seeds)")
    plot_cdf_arrival_times_per_block_median_over_seeds(
        seed_paths,
        arrival_col="Seed Part Arrival Times",
        title="CDF of seed-part arrival times per block",
        probs_points=200,
        max_points_per_seed_per_block=None,
    )
    plt.show()

    print("\n[Plot] CDF per block: Sample Arrival Times (median over seeds)")
    plot_cdf_arrival_times_per_block_median_over_seeds(
        seed_paths,
        arrival_col="Sample Arrival Times",
        title="CDF of sample arrival times per block",
        probs_points=200,
        max_points_per_seed_per_block=200_000,
    )
    plt.show()


