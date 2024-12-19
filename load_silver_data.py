"""
Fetches data from the Cassandra `bronze_tracks` table, cleans the data, and inserts it into the `silver_tracks` table.

- **Data Fetching**: Retrieves all records from the `bronze_tracks` table.
- **Data Cleaning**: 
    - Removes duplicates based on `track_id`.
    - Replaces empty strings with `None` and drops rows with missing values.
    - Converts the `explicit` column to 1 or 0.
    - Drops the `instrumentalness` column if it exists.
- **Data Insertion**: Inserts cleaned data into the `silver_tracks` table.
- **Error Handling**: Skips rows with insertion errors.
- **Progress Logging**: Logs progress every 1000 records.
- **Record Count**: After insertion, retrieves and prints the total record count in the table.
"""
from cassandra_connection import get_session
import pandas as pd
from cassandra.query import BatchStatement

session = get_session()

select_query = "SELECT * FROM bronze_tracks;"
rows_bronze = session.execute(select_query)
bronze_data = list(rows_bronze)

columns = ['unique_id', 'track_id', 'artists', 'album_name', 'track_name',
           'popularity', 'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
           'mode', 'speechiness', 'acousticness','instrumentalness','liveness', 'valence', 'tempo', 
           'time_signature', 'track_genre']

df_bronze = pd.DataFrame([{col: getattr(row, col, None) for col in columns} for row in bronze_data])


data = df_bronze.drop_duplicates(subset=['track_id'])
data = data.replace("", None)
data = data.dropna()


data['explicit'] = data['explicit'].apply(lambda x: 1 if x else 0)
if 'instrumentalness' in data.columns:
    data = data.drop(columns=['instrumentalness'])

print(f"Data before cleaning: {df_bronze.shape}")
print(f"Data after cleaning: {data.shape}")

batch = BatchStatement()
batch_size_limit = 1000
batch_count = 0

insert_query = """
    INSERT INTO silver_tracks (
        unique_id, track_id, artists, album_name, track_name, popularity, duration_ms,
        explicit, danceability, energy, key, loudness, mode, speechiness,
        acousticness, liveness, valence, tempo, time_signature, track_genre
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""

inserted_count = 0
total_records = len(data)

for _, row in data.iterrows():
    batch.add(insert_query, (
        row['unique_id'], row['track_id'], row['artists'], row['album_name'], row['track_name'],
        row['popularity'], row['duration_ms'], row['explicit'], row['danceability'],
        row['energy'], row['key'], row['loudness'], row['mode'], row['speechiness'],
        row['acousticness'], row['liveness'], row['valence'],
        row['tempo'], row['time_signature'], row['track_genre']
    ))

    inserted_count += 1
    if inserted_count % batch_size_limit == 0 or inserted_count == total_records:
        try:
            session.execute(batch)
            print(f"Inserting batch {batch_count + 1}/{(total_records // batch_size_limit) + 1}...")
            batch_count += 1
        except Exception as e:
            print(f"Error executing batch {batch_count + 1}: {e}")
        
        batch = BatchStatement()

print("Data inserted successfully into the silver_tracks table.")

count_query = "SELECT COUNT(*) FROM silver_tracks;"
result = session.execute(count_query)
for row in result:
    print(f"Total number of records in the silver_tracks table: {row[0]}")
