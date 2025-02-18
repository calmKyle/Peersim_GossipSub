import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the CSV file
data = pd.read_csv("output1.csv")

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

# Load data from CSV file (Replace 'your_file.csv' with the actual filename)
data = pd.read_csv('output1.csv')

# Ensure 'Total Sample Received' exists before computing 'Percent Received'
if 'Total Sample Received' not in data.columns:
    raise ValueError("Error: 'Total Sample Received' column not found in the CSV file.")

# 🔹 Compute 'Percent Received' (Total samples received out of 73)
data['Percent Received'] = (data['Total Sample Received'] / 73) * 100

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



#############################
# Sample request / recieved percentage 
import matplotlib.pyplot as plt
import pandas as pd

# Load data from CSV file
data = pd.read_csv('output1.csv')

# Assuming columns 'Total Sample Req Sent' and 'Total Sample Received' are in the DataFrame
data['Percent Received over Requested'] = (data['Total Sample Received'] / data['Total Sample Req Sent']) * 100
data['Total Sample Req Timedout'] = 100 - data['Percent Received over Requested']  # Assuming no other losses

# Create a figure with two subplots side by side
fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 6))

# Plotting Percent Received over Requested and Total Sample Req Timedout on the first subplot
axes[0].bar(data.index, data['Percent Received over Requested'], color='blue', label='Percent Received over Requested')
axes[0].bar(data.index, data['Total Sample Req Timedout'], color='red', bottom=data['Percent Received over Requested'], label='Total Sample Req Timedout')
axes[0].set_title('Percentage of Samples Received and Lost over Samples Requested')
axes[0].set_xlabel('Sample Index')
axes[0].set_ylabel('Percentage (%)')
axes[0].set_ylim(0, 100)  # Ensuring y-limit is up to 100% for clarity
axes[0].set_yticks(range(0, 101, 10))  # Setting y-ticks at 10% intervals
axes[0].legend()

# Calculate 'Percent Received over 512' and plot on the second subplot
data['Percent Received over 512'] = (data['Total Sample Received'] / 512) * 100
axes[1].bar(data.index, data['Percent Received over 512'], color='green')
axes[1].set_title('Percentage of Samples Received over 512')
axes[1].set_xlabel('Sample Index')
axes[1].set_ylabel('Percentage (%)')
axes[1].set_ylim(0, 100)  # Ensuring y-limit is up to 100% for clarity
axes[1].set_yticks(range(0, 101, 10))  # Setting y-ticks at 10% intervals

# Improve layout and show plot
plt.tight_layout()
plt.show()
