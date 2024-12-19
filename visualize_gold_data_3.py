"""
Visualizes high danceability tracks by plotting energy against danceability with loudness as a color gradient.

Steps:
1. Fetches aggregated high danceability track data from the `golden_high_danceability_tracks_aggregated` table.
2. Creates a scatter plot using Matplotlib to show:
   - Danceability (x-axis)
   - Average energy (y-axis)
   - Average loudness (color gradient)

Dependencies: `pandas`, `matplotlib`, `cassandra_connection`
"""


import matplotlib.pyplot as plt
import pandas as pd
from cassandra_connection import get_session

session = get_session()

# Scatter Plot: High Danceability Tracks vs Average Energy
high_danceability_query = "SELECT * FROM golden_high_danceability_tracks_aggregated;"
rows = session.execute(high_danceability_query)
danceability_data = pd.DataFrame(rows)

plt.figure(figsize=(10, 6))
plt.scatter(danceability_data['danceability'], danceability_data['avg_energy'], 
            c=danceability_data['avg_loudness'], cmap='viridis', alpha=0.6, edgecolors='w')
plt.colorbar(label='Average Loudness')
plt.xlabel('Danceability')
plt.ylabel('Average Energy')
plt.title('High Danceability Tracks: Energy vs Danceability')
plt.tight_layout()
plt.show()
