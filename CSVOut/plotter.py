#!/usr/bin/env python3
"""
plot_rtt.py – Visualise Max Seed RTT vs. malicious rate

• Reads CSV files whose name contains “…malicious_rate_<rate>…”
  (rate can be 0, 0.1, 0.2, …; seed id can vary).
• Merges every seed that belongs to the same rate.
• Figure 1: CDF of Max Seed RTT – one stepped line per rate.
            Legend labels are clickable: clicking toggles visibility.
• Figure 2: “Patch‑line” – average Max Seed RTT for each rate.
"""

import glob
import re
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ---------------------------------------------------------------------------
# CONFIG – tweak if your filenames vary
# ---------------------------------------------------------------------------
FILE_PATTERN = 'output_results_malicious_rate_*.csv'
RATE_REGEX   = re.compile(r'_rate_([0-9]+(?:\.[0-9]+)?)')   # captures “0”, “0.1” …
THRESHOLD_MS = 4000
XMAX_CDF     = 5000


# ---------------------------------------------------------------------------
# 1.  READ CSVs  ➜  bucket each seed under its malicious‑rate
# ---------------------------------------------------------------------------
rtt_by_rate = defaultdict(list)     # {rate → [np.array, …]}

for file in glob.glob(FILE_PATTERN):
    m = RATE_REGEX.search(file)
    if not m:
        print(f"⚠️  Skipping (no rate in name): {file}")
        continue

    rate = float(m.group(1))
    df   = pd.read_csv(file)

    if 'Max Seed Part RTT' not in df.columns:
        print(f"⚠️  Skipping (missing column): {file}")
        continue

    rtt_by_rate[rate].append(df['Max Seed Part RTT'].values)

if not rtt_by_rate:
    raise SystemExit("No matching CSV files found – check FILE_PATTERN or folder.")


# ---------------------------------------------------------------------------
# 2.  FIGURE 1 – interactive CDF  (one‑seed normalised)
# ---------------------------------------------------------------------------
fig_cdf, ax_cdf = plt.subplots(figsize=(10, 7))
cdf_lines       = []

for rate in sorted(rtt_by_rate):
    xs   = np.sort(np.concatenate(rtt_by_rate[rate]))          # merge all seeds
    n_seeds = len(rtt_by_rate[rate])                           # how many seeds?
    
    # ――― Y‑values now represent the *average* over seeds ―――
    # Each seed contributes exactly 1 node to the count, so the final
    # value on the y‑axis ends at 16 384 instead of n_seeds*16 384.
    ys = np.arange(1, len(xs) + 1) / n_seeds

    line, = ax_cdf.step(xs, ys, where='post', label=f'rate = {rate:g}')
    cdf_lines.append(line)

ax_cdf.set(
    title='CDF of Max Seed RTT (average across seeds; 16 384 nodes per seed)',
    xlabel='Time (ms)',
    ylabel='Average number of nodes',
    xlim=(0, XMAX_CDF)
)
ax_cdf.axvline(THRESHOLD_MS, color='red', ls='--',
               label=f'{THRESHOLD_MS} ms threshold')
ax_cdf.grid(True)

legend       = ax_cdf.legend(loc='best', title='Click a label to hide/show')
legend_lines = legend.get_lines()          # portable across Matplotlib versions
for leg_line in legend_lines:
    leg_line.set_picker(True)
    leg_line.set_alpha(1.0)


def on_pick(event):
    leg_line = event.artist
    if leg_line not in legend_lines:
        return
    idx        = legend_lines.index(leg_line)
    data_line  = cdf_lines[idx]
    visible    = not data_line.get_visible()
    data_line.set_visible(visible)
    leg_line.set_alpha(1.0 if visible else 0.25)
    fig_cdf.canvas.draw_idle()


fig_cdf.canvas.mpl_connect('pick_event', on_pick)
fig_cdf.tight_layout()


# ---------------------------------------------------------------------------
# 3.  FIGURE 2 – highest RTT (+2 ms); hide < 100 ms or ≥ 4 000 ms
# ---------------------------------------------------------------------------
deadline_ms   = 4000        # 4‑s deadline
min_valid_ms  = 100         # hide everything below this

# Add 2 ms to each rate’s single worst RTT
max_rtt_plus2 = {r: np.max(np.concatenate(v)) + 2
                 for r, v in rtt_by_rate.items()}

# Keep only points in the allowed band [100 ms, 4 000 ms)
valid_rates, valid_vals = [], []
for r, val in max_rtt_plus2.items():
    if min_valid_ms <= val < deadline_ms:
        valid_rates.append(r)
        valid_vals.append(val)

fig_max, ax_max = plt.subplots(figsize=(8, 5))

if valid_rates:  # plot only if something survives the filter
    ax_max.plot(valid_rates, valid_vals, marker='o', lw=2,
                label='max RTT + 2 ms')

# Always show the 4‑s deadline
ax_max.axhline(deadline_ms, color='red', ls='--', label='4 000 ms deadline')

ax_max.set(
    title='Highest Max Seed RTT (+2 ms)\n(points < 100 ms or ≥ 4 000 ms hidden)',
    xlabel='Malicious rate (fraction)',
    ylabel='Highest Max Seed RTT (ms)'
)
ax_max.set_xticks(sorted(valid_rates))
ax_max.grid(True)
ax_max.legend()
fig_max.tight_layout()
# ---------------------------------------------------------------------------
# 4.  SHOW
# ---------------------------------------------------------------------------
plt.show()

