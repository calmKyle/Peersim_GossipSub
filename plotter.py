# Violin Graph
import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
data = pd.read_csv("output1.csv")

import seaborn as sns
import matplotlib.pyplot as plt

# Create the violin plot
plt.figure(figsize=(10, 6))
sns.violinplot(data=data['Max Sample RTT'])

# data['parsed_times'] = data["Sample Arrival Times"].apply(lambda x: list(map(float, x.split(';'))))



# # Step 3: Prepare data for the violin plot
# # Convert the lists to an array where each entry represents data for a node
# violin_data = data['parsed_times'].tolist()

# Step 4: Plot the violin chart
# plt.figure(figsize=(12, 6))
# plt.violinplot(violin_data, showmeans=True, showmedians=True)

# Add labels and title
plt.xlabel('Nodes')
plt.ylabel('Arrival Time (ms)')
plt.title('Last Sample Arrival Time Distribution Across Nodes')

# Show the plot
plt.show()
# ##################################################################################
# import pandas as pd
# import matplotlib.pyplot as plt

# # Load the data
# data = pd.read_csv("output1.csv")

# # Calculate the percentage of messages received
# data['Percent Received'] = (data['Total Sample Received'] / 75) * 100  # Assuming 75 messages is the total

# plt.figure(figsize=(16, 8))

# # Plot without log scale
# plt.plot(data['Node ID'], data['Percent Received'], color='green', marker='o', linestyle='-', linewidth=0.8, markersize=3, alpha=0.7)

# # Adjust the x-axis ticks and limits
# plt.xlim(1, len(data['Node ID']) + 10)  # x-axis range
# plt.xticks(range(1, len(data['Node ID']) + 1, 50))  # Adjust the step as needed

# # Labels and title
# plt.xlabel("Node ID")
# plt.ylabel("Percentage of Messages Received (%)")
# plt.title("Line Graph of Message Reception Percentage(Number of samples received out of 75)")

# # Add grid for better readability
# plt.grid(True)

# # Show the plot
# plt.show()


##################################################################
# ## Box and whisper graph
# import pandas as pd
# import matplotlib.pyplot as plt
# from matplotlib import cbook
# import numpy as np

# # Step 1: Read the CSV file
# df = pd.read_csv("output1.csv")

# # Step 2: Convert "Sample Arrival Times" to lists of integers
# df["Sample Arrival Times"] = df["Sample Arrival Times"].apply(
#     lambda x: list(map(int, x.split(';'))) if isinstance(x, str) and x.strip() else []
# )

# # Step 3: Explode the lists into individual rows
# df_long = df.explode("Sample Arrival Times")

# # Step 4: Ensure the column is numeric
# df_long["Sample Arrival Times"] = pd.to_numeric(df_long["Sample Arrival Times"])

# # Step 5: Group nodes into chunks of 16
# node_ids = df["Node ID"].unique()
# chunk_size = 100
# chunks = [node_ids[i:i + chunk_size] for i in range(0, len(node_ids), chunk_size)]

# # Step 6: Create subplots for each chunk
# for i, chunk in enumerate(chunks):
#     # Calculate statistics for the current chunk
#     stats = cbook.boxplot_stats(
#         [df_long[df_long["Node ID"] == node]["Sample Arrival Times"].values for node in chunk],
#         labels=chunk
#     )
    
#     # Create a new subplot for this chunk
#     fig, ax = plt.subplots(figsize=(14, 7))
#     ax.bxp(stats, patch_artist=True, boxprops={'facecolor': 'lightblue'})
    
#     # Customize the plot
#     ax.set_title(f"Sample Arrival Times Box Plot (Nodes {chunk[0]} to {chunk[-1]})", fontsize=16)
#     ax.set_xlabel("Node ID", fontsize=12)
#     ax.set_ylabel("Sample Arrival Times (ms)", fontsize=12)
#     plt.xticks(rotation=45)  # Rotate x-axis labels for readability
    
#     # Show the plot
#     plt.tight_layout()
#     plt.show()

#####################################
##Bar graph
import pandas as pd
import matplotlib.pyplot as plt

# Load the data
data = pd.read_csv("output1.csv")

# Calculate the percentage of messages received
data['Percent Received'] = (data['Total Sample Received'] / 75) * 100  # Assuming 75 messages is the total

# Function to categorize based on percentage
def categorize_percent(percent):
    if percent < 90:
        return 'Less than 90%'
    elif percent == 90:
        return '90%'
    elif percent == 91:
        return '91%'
    elif percent == 92:
        return '92%'
    elif percent == 93:
        return '93%'
    elif percent == 94:
        return '94%'
    elif percent == 95:
        return '95%'
    elif percent == 96:
        return '96%'
    elif percent == 97:
        return '97%'
    elif percent == 98:
        return '98%'
    elif percent == 99:
        return '99%'
    elif percent == 100:
        return '100%'

# Apply the function to create the 'Category' column
data['Category'] = data['Percent Received'].apply(categorize_percent)

# Count the number of nodes in each category
category_counts = data['Category'].value_counts().sort_index()

# Plot as a bar graph
plt.figure(figsize=(12, 6))
plt.bar(category_counts.index, category_counts.values, color='green', alpha=0.7, width=0.8)

# Labels and title
plt.xlabel("Percentage of Samples Received")
plt.ylabel("Number of Nodes")
plt.title("Nodes with Sample Reception Percentage from Less than 90% to 100% in 4s")

# Add grid for better readability
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Show the plot
plt.show()
