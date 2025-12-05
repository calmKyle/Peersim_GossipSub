

import os
import matplotlib.pyplot as plt  # make sure this is imported before the function

# Ask the user where to save figures
output_dir = input("Enter the folder path where you want to save all the figures: ").strip()
os.makedirs(output_dir, exist_ok=True)

plot_index = 1

def save_figure(name):
    global plot_index
    file_path = os.path.join(output_dir, f"{plot_index:02d}_{name}.png")
    plt.savefig(file_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ Saved: {file_path}")
    plot_index += 1
#################################################
# Get data from conf file
import re

# Path to your configuration file
config_file_path = "GossipConfig.cfg"

# Dictionary to hold extracted key-value pairs
config_values = {}

# Regular expression pattern to match key-value pairs
pattern = re.compile(r'^\s*([^#\s]+)\s+([^\s#]+)')

# Read and parse the configuration file
with open(config_file_path, 'r') as file:
    for line in file:
        # Ignore comments and empty lines
        if line.strip().startswith("#") or not line.strip():
            continue

        # Match key-value pairs using regex
        match = pattern.match(line)
        if match:
            key, value = match.groups()
            config_values[key] = value

# Display extracted values
for key, value in config_values.items():
    print(f"{key} = {value}")

sample_amount = int(config_values.get("SAMPLE_AMOUNT"))
import pandas as pd
import matplotlib.pyplot as plt

# 1. Read the CSV file
df = pd.read_csv("output_results.csv")

# 2. Strip leading/trailing spaces from column names (if needed)
df.columns = df.columns.str.strip()

# 3. Create a figure
plt.figure(figsize=(8, 6))

# 4. Plot two lines, each with a label for the legend
plt.plot(df["Node ID"], df["Duplicated Message IHAVE"], marker="o", label="Duplicated Data")
# plt.plot(df["Node ID"], df["Total Sample Req Sent"], marker="x", label="Total Sample Req Sent")

# 5. Label axes and add a title
plt.xlabel("Node ID")
plt.ylabel("Value")
plt.title("Duplicate Data with Sharding Distribution")

# 6. Show the legend so we know which line is which
plt.legend()

# 7. Display the chart
save_figure("Figure_6")
