"""
Visualizes the top music genres by average popularity from a Cassandra database.

Steps:
1. Fetches data from the `golden_avg_popularity_by_genre` table using a Cassandra session.
2. Processes the data to extract the top 10 genres by average popularity.
3. Plots a bar chart using Matplotlib.

Dependencies: `matplotlib`, `pandas`, `cassandra_connection`
"""


import matplotlib.pyplot as plt
import pandas as pd
from cassandra_connection import get_session

session = get_session()

# Bar Plot: Top 10 Genres by Average Popularity
avg_popularity_query = "SELECT * FROM golden_avg_popularity_by_genre;"
rows = session.execute(avg_popularity_query)
popularity_data = pd.DataFrame(rows, columns=['track_genre', 'avg_popularity'])
top_10_popularity = popularity_data.sort_values(by='avg_popularity', ascending=False).head(20)


plt.figure(figsize=(10, 6))
plt.bar(top_10_popularity['track_genre'], top_10_popularity['avg_popularity'], color='skyblue')
plt.xlabel('Track Genre')
plt.ylabel('Average Popularity')
plt.title('Top 10 Genres by Average Popularity')
plt.xticks(rotation=90, ha='right')
plt.tight_layout()
plt.show()

