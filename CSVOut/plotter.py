
#############################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob

# Pattern to match all your CSV files
csv_files = glob.glob('output_results_malicious_rate_*.csv')

# --- Plot for Seed RTT ---
plt.figure(figsize=(10, 7))

for file in csv_files:
    # Extract the malicious rate from filename for labeling
    malicious_rate = file.split('_')[-1].replace('.csv', '')
    
    # Load CSV
    df = pd.read_csv(file)
    
    # Get 'Max Seed RTT'
    times_seed = df['Max Seed RTT']
    
    # Sort and compute CDF
    data_sorted_seed = np.sort(times_seed)
    cumulative_node_count_seed = np.arange(1, len(data_sorted_seed) + 1)

    # Plot Seed Arrival Times
    plt.step(data_sorted_seed, cumulative_node_count_seed, where='post', label=f'Seed RTT (rate={malicious_rate})')

# Decorations
plt.title('CDF of Max Seed RTT (across multiple malicious rates)')
plt.xlabel('Time (ms)')
plt.ylabel('Number of Nodes')
plt.xlim(0, 5000)
plt.axvline(x=4000, color='red', linestyle='--', label='Threshold 4000 ms')
plt.legend()
plt.grid(True)
plt.show()

# --- Plot for Sample RTT ---
plt.figure(figsize=(10, 7))

for file in csv_files:
    # Extract the malicious rate from filename for labeling
    malicious_rate = file.split('_')[-1].replace('.csv', '')
    
    # Load CSV
    df = pd.read_csv(file)
    
    # Get 'Max Sample RTT'
    times_sample = df['Max Sample RTT']
    
    # Sort and compute CDF
    data_sorted_sample = np.sort(times_sample)
    cumulative_node_count_sample = np.arange(1, len(data_sorted_sample) + 1)

    # Plot Sample RTT
    plt.step(data_sorted_sample, cumulative_node_count_sample, where='post',  label=f'Sample RTT (rate={malicious_rate})')

# Decorations
plt.title('CDF of Max Sample RTT (across multiple malicious rates)')
plt.xlabel('Time (ms)')
plt.ylabel('Number of Nodes')
plt.xlim(0, 5000)
plt.axvline(x=4000, color='red', linestyle='--', label='Threshold 4000 ms')
plt.legend()
plt.grid(True)
plt.show()

