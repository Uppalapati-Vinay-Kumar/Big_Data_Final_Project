"""
Visualizes the top 10 music genres by average danceability from a Cassandra database.

Steps:
1. Fetches feature averages for genres from the `golden_genre_feature_averages` table.
2. Processes the data to extract the top 10 genres based on average danceability.
3. Plots a bar chart using Matplotlib.

Dependencies: `pandas`, `matplotlib`, `cassandra_connection`
"""

import pandas as pd
import matplotlib.pyplot as plt
from cassandra_connection import get_session

session = get_session()

cql_query = """
SELECT track_genre, avg_danceability, avg_energy, avg_loudness, avg_tempo, avg_valence
FROM golden_genre_feature_averages;
"""
rows = session.execute(cql_query)
data = [dict(row._asdict()) for row in rows]
df = pd.DataFrame(data)

session.shutdown()

top_genres = df.sort_values(by='avg_danceability', ascending=False).head(10)

plt.figure(figsize=(10, 6))
plt.bar(top_genres['track_genre'], top_genres['avg_danceability'], color='skyblue')
plt.xticks(rotation=45, ha="right")
plt.title("Top 10 Genres by Average Danceability")
plt.ylabel("Average Danceability")
plt.xlabel("Track Genre")
plt.tight_layout()
plt.show()
