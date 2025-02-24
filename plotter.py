#########################################################################
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the CSV file
data = pd.read_csv("output2.csv")

# Check if necessary columns exist
if 'Max Sample RTT' not in data.columns or 'Avg Sample RTT' not in data.columns:
    raise ValueError("CSV must include 'Max Sample RTT' and 'Avg Sample RTT' columns.")

# Prepare data for violin plot
# Create a new DataFrame for plotting
plot_data = pd.DataFrame({
    'RTT Value': pd.concat([data['Max Sample RTT'], data['Avg Sample RTT']], ignore_index=True),
    'Type': ['Max'] * len(data['Max Sample RTT']) + ['Avg'] * len(data['Avg Sample RTT'])
})

# Create the violin plot
plt.figure(figsize=(10, 6))
sns.violinplot(x='Type', y='RTT Value', data=plot_data, palette='muted')

# Add labels and title
plt.xlabel('RTT Type')
plt.ylabel('RTT (ms)')
plt.title('Distribution of Max and Avg Sample RTT Across Samples')

# Show the plot
plt.show()


# #####################################
# ##Bar graph
import matplotlib.pyplot as plt
import pandas as pd
import re

# Path to your configuration file
config_file_path = "GossipConfig.cfg"  # Replace with your actual file path

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


# Load data from CSV file (Replace 'your_file.csv' with the actual filename)
data = pd.read_csv('output2.csv')
sample_amount = int(config_values.get("SAMPLE_AMOUNT"))
# Ensure 'Total Sample Received' exists before computing 'Percent Received'
if 'Total Sample Received' not in data.columns:
    raise ValueError("Error: 'Total Sample Received' column not found in the CSV file.")

# 🔹 Compute 'Percent Received' (Total samples received out of 73)
data['Percent Received'] = (data['Total Sample Received'] / sample_amount) * 100

# 🔹 Categorization function
def categorize_percent(percent):
    percent = int(percent)  # Convert float to integer for cleaner categories
    if percent >= 50:
        return f"{percent}%"
    return "Less than 50%"

# 🔹 Apply function to create 'Category' column
data['Category'] = data['Percent Received'].apply(categorize_percent)

# 🔹 Define the correct order for sorting (100% → 50% → Less than 50%)
category_order = [f"{i}%" for i in range(100, 70, -1)] + ["Less than 50%"]

# 🔹 Count the number of nodes in each category (preserve order)
category_counts = data['Category'].value_counts().reindex(category_order, fill_value=0)

# 🔹 Plot as a bar graph
plt.figure(figsize=(12, 6))
plt.bar(category_counts.index, category_counts.values, color='green', alpha=0.7, width=0.8)

# 🔹 Labels and title
plt.xlabel("Percentage of Samples Received")
plt.ylabel("Number of Nodes")
plt.title("Nodes with Sample Reception Percentage from 100% to 70% and Less than 50% in 4s")

# 🔹 Rotate x-axis labels for readability
plt.xticks(rotation=90)

# 🔹 Add grid for better readability
plt.grid(axis='y', linestyle='--', alpha=0.7)

# 🔹 Show the plot
plt.show()



# #############################
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import re

# Path to your configuration file
config_file_path = "GossipConfig.cfg"  # Replace with your actual file path

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


# Load data
data = pd.read_csv('output2.csv')

# Ensure valid numerical computations
data['Total Sample Req Sent'].replace(0, np.nan, inplace=True)  # Avoid divide-by-zero errors

# Compute necessary columns
data['Percent Received over Requested'] = (data['Total Sample Received'] / data['Total Sample Req Sent']) * 100
data['Total Sample Req Timedout'] = 100 - data['Percent Received over Requested']
data['Percent Received over 512'] = (data['Total Sample Received'] / 512) * 100

# **Downsampling strategy** (Only apply if dataset is large)
max_points = 2000  # Maximum points to display
if len(data) > max_points:
    step = max(len(data) // max_points, 1)  # Ensure step is at least 1
    data = data.iloc[::step, :].reset_index(drop=True)  # Reset index after sampling

# Create figure
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# **First plot: Percent Received vs. Timedout**
axes[0].plot(data.index, data['Percent Received over Requested'], color='blue', label='Percent Received', alpha=0.7)
axes[0].fill_between(data.index, data['Percent Received over Requested'], 100, color='red', alpha=0.5, label='Timed Out')
axes[0].set_title('Percentage of Samples Received vs. Requested')
axes[0].set_xlabel('Sample Index')
axes[0].set_ylabel('Percentage (%)')
axes[0].set_ylim(0, 100)
axes[0].legend()
axes[0].grid(True, linestyle='--', alpha=0.5)

# Set x-ticks dynamically to avoid clutter
num_ticks = min(10, len(data))  # Limit number of ticks
xtick_positions = np.linspace(0, len(data) - 1, num_ticks, dtype=int)
axes[0].set_xticks(xtick_positions)

# **Second plot: Percent Received over 512**
axes[1].plot(
    data.index, 
    data['Percent Received over 512'], 
    color='green', 
    lw=1.5, 
    alpha=0.7, 
    label='Percent Received over 512'
)

# Fill the area below the line
axes[1].fill_between(
    data.index, 
    data['Percent Received over 512'], 
    color='green', 
    alpha=0.3  # Adjust transparency (0 = fully transparent, 1 = fully opaque)
)

# Set plot titles and labels
axes[1].set_title('Percentage of Samples Received over 512')
axes[1].set_xlabel('Sample Index')
axes[1].set_ylabel('Percentage (%)')
axes[1].set_ylim(0, 100)
axes[1].grid(True, linestyle='--', alpha=0.5)

# Add a legend
axes[1].legend(loc='lower right')


# Set x-ticks dynamically
axes[1].set_xticks(xtick_positions)

# **Save plot instead of showing in headless environments**
plt.tight_layout()
# plt.savefig('output.png', dpi=300, bbox_inches='tight')
# print("Plot saved as output.png")

# **Show plot only if running in an interactive environment**
try:
    plt.show()
except Exception:
    print("Matplotlib is running in a headless environment; use plt.savefig() instead.")



######################################
# import matplotlib.pyplot as plt
# import pandas as pd
# import numpy as np
# import matplotlib.cm as cm

# # Load the CSV file
# df = pd.read_csv('output2.csv')  # Replace with your actual file path

# # Randomly select 200 rows
# random_rows = df["Sample Arrival Times"].sample(n=200, random_state=42)

# # Generate a colormap with 200 unique colors
# colormap = cm.get_cmap("tab20", 200)  # "tab20" provides diverse colors

# # Create a figure for multiple CDF plots
# plt.figure(figsize=(10, 5))

# # --- Plot Individual CDFs ---
# for i, row in enumerate(random_rows):
#     # Convert the semicolon-separated string into a list of integers
#     arrival_times = list(map(int, row.split(';')))

#     # Sort data
#     sorted_data = np.sort(arrival_times)

#     # Compute CDF
#     cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)

#     # Plot individual CDF with dynamically generated colors
#     plt.plot(sorted_data, cdf, linestyle='-', color=colormap(i), alpha=0.5)

# # Formatting the plot
# plt.xlabel("Operation completion time (ms)")
# plt.ylabel("CDF")
# plt.grid(True)

# # Highlighting a vertical threshold
# threshold = 4000
# plt.axvline(x=threshold, color='red', linestyle='--', label=f'Threshold {threshold} ms')

# # Show legend and title
# plt.legend(title="CDFs of 200 Rows", loc="lower right")
# plt.title("Random 200 Rows CDFs")

# # Show the plot
# plt.show()


######################################################
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib.cm as cm

# Load the CSV file
df = pd.read_csv('output2.csv')  # Replace with your actual file path

# Randomly select 200 rows
random_rows = df["Sample Arrival Times"].sample(n=200, random_state=42)

# Generate a colormap with 200 unique colors
colormap = cm.get_cmap("tab20", 200)  # "tab20" provides diverse colors

# Store CDFs for median calculation
all_cdfs = []
all_x_values = []

# Create a figure for multiple CDF plots
plt.figure(figsize=(10, 5))

# --- Plot Individual CDFs ---
for i, row in enumerate(random_rows):
    # Convert the semicolon-separated string into a list of integers
    arrival_times = list(map(int, row.split(';')))

    # Sort data
    sorted_data = np.sort(arrival_times)

    # Compute CDF
    cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)

    # Store x-values and CDFs for median calculation
    all_x_values.append(sorted_data)
    all_cdfs.append(cdf)

    # Plot individual CDF with dynamically generated colors
    plt.plot(sorted_data, cdf, linestyle='-', color=colormap(i), alpha=0.5)

# --- Median CDF Calculation ---
# Define a common x-axis range (linear space of operation times)
common_x = np.linspace(min(map(np.min, all_x_values)), max(map(np.max, all_x_values)), 1000)

# Interpolate each CDF to the common x-values
interpolated_cdfs = []
for i in range(len(all_cdfs)):
    x_values = np.clip(all_x_values[i], common_x[0], common_x[-1])
    interpolated_cdf = np.interp(common_x, x_values, all_cdfs[i])
    interpolated_cdfs.append(interpolated_cdf)

# Convert to numpy array for easier calculations
interpolated_cdfs = np.array(interpolated_cdfs)

# Compute the median across interpolated x-values
median_cdf = np.median(interpolated_cdfs, axis=0)

# Draw the median only between CDF=0.1 to CDF=0.9
lower_bound = 0.004
upper_bound = 0.99999999
mask = (median_cdf >= lower_bound) & (median_cdf <= upper_bound)

# Plot the median CDF only in the middle region
plt.plot(common_x[mask], median_cdf[mask], linestyle='-', color='red', linewidth=2, label='Median CDF (Middle)')

# Formatting the plot
plt.xlabel("Operation completion time (ms)")
plt.ylabel("CDF")
plt.grid(True)

# Set custom axis limits
plt.xlim(0, 5000)   
plt.ylim(0, 1.1)  

# Highlighting a vertical threshold
threshold = 4000
plt.axvline(x=threshold, color='red', linestyle='--', label=f'Threshold {threshold} ms')

# Show legend and title
plt.legend(title="CDFs of n Rows", loc="lower right")
plt.title("Random n Rows CDFs with Median Focused on Middle Region")

# Show the plot
plt.show()
