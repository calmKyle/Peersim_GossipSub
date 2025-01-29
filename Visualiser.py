import pandas as pd
import matplotlib.pyplot as plt

# Load your simulation results
data = pd.read_csv('simulation_results.csv')

# Plotting example: Chain height over time for different fork choice strategies
plt.figure(figsize=(12, 6))
for strategy in data['strategy'].unique():
    subset = data[data['strategy'] == strategy]
    plt.plot(subset['time'], subset['chain_height'], label=strategy)

plt.title('Blockchain Height Over Time by Fork Choice Strategy')
plt.xlabel('Time (simulation ticks)')
plt.ylabel('Chain Height')
plt.legend()
plt.show()

