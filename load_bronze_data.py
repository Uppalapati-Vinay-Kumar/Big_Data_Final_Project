"""
Reads track data from a CSV file and inserts it into the Cassandra bronze_tracks table.

- **Data Loading**: Reads data from 'spotify.csv', drops the first column, and fills missing values.
- **Data Insertion**: Inserts the cleaned data into the `bronze_tracks` table in Cassandra in batches.
- **Error Handling**: Skips rows with errors during insertion.
- **Progress Logging**: Logs progress every 1000 records.
- **Record Count**: After insertion, retrieves and prints the total record count in the table.
"""
from cassandra_connection import get_session
import pandas as pd
import uuid
from cassandra.query import BatchStatement

session = get_session()

csv_file = "received_spotify_data.csv"
data = pd.read_csv(csv_file)
data = data.drop(data.columns[0], axis=1)
data = data.fillna('')
print(data.head())
print(data.shape)

insert_query = """
    INSERT INTO bronze_tracks (
        unique_id, track_id, artists, album_name, track_name, popularity, duration_ms,
        explicit, danceability, energy, key, loudness, mode, speechiness,
        acousticness, instrumentalness, liveness, valence, tempo, 
        time_signature, track_genre
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""

batch = BatchStatement()
batch_size_limit = 1000
batch_count = 0

for index, row in data.iterrows():
    batch.add(insert_query, (
        uuid.uuid4(), row['track_id'], row['artists'], row['album_name'], row['track_name'],
        row['popularity'], row['duration_ms'], row['explicit'], row['danceability'],
        row['energy'], row['key'], row['loudness'], row['mode'], row['speechiness'],
        row['acousticness'], row['instrumentalness'], row['liveness'], row['valence'],
        row['tempo'], row['time_signature'], row['track_genre']
    ))

    if (index + 1) % batch_size_limit == 0 or (index + 1) == data.shape[0]:
        try:
            session.execute(batch)
            print(f"Inserting batch {batch_count + 1}/{(data.shape[0] // batch_size_limit) + 1}...")
            batch_count += 1
        except Exception as e:
            print(f"Error executing batch {batch_count + 1}: {e}")
        
        batch = BatchStatement()

print("Data inserted successfully into the bronze_tracks table.")

count_query = "SELECT COUNT(*) FROM bronze_tracks;"
result = session.execute(count_query)
for row in result:
    print(f"Total number of records in the bronze_tracks table: {row[0]}")

