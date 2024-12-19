"""
Fetches data from the `silver_tracks` table, computes averages for popularity and track features by genre, 
and inserts the results into various 'golden' tables.

- **Data Retrieval**: 
    - Fetches relevant track details (genre, popularity, danceability, etc.) from the `silver_tracks` table.
  
- **Calculations**:
    - **Average Popularity**: Computes the average popularity by genre.
    - **Average Features**: Calculates the average values for features such as danceability, energy, loudness, tempo, and valence by genre.
  
- **Data Insertion**: 
    - Inserts the calculated average popularity into the `golden_avg_popularity_by_genre` table.
    - Inserts the calculated feature averages into the `golden_genre_feature_averages` table.
    - Identifies high danceability tracks (danceability > 0.8) and inserts them into the `golden_high_danceability_tracks_aggregated` table.
  
- **Error Handling**: Logs any errors during the insertion of data into the three golden tables.
"""


from cassandra_connection import get_session
from cassandra.query import BatchStatement

session = get_session()

query = "SELECT track_genre, track_id, popularity, danceability, energy, loudness, tempo, valence, artists, album_name, track_name FROM silver_tracks;"
rows = session.execute(query)

genre_popularity = {}
genre_features = {}
genre_counts = {}

for row in rows:
    genre = row[0] 
    popularity = row[2] 
    danceability = row[3]  
    energy = row[4]  
    loudness = row[5]  
    tempo = row[6]  
    valence = row[7]  
    artists = row[8]  
    album_name = row[9]  
    track_name = row[10]  

    if genre in genre_popularity:
        genre_popularity[genre] += popularity
        genre_counts[genre] += 1
    else:
        genre_popularity[genre] = popularity
        genre_counts[genre] = 1

    if genre not in genre_features:
        genre_features[genre] = {
            'total_danceability': 0,
            'total_energy': 0,
            'total_loudness': 0,
            'total_tempo': 0,
            'total_valence': 0
        }
    
    genre_features[genre]['total_danceability'] += danceability
    genre_features[genre]['total_energy'] += energy
    genre_features[genre]['total_loudness'] += loudness
    genre_features[genre]['total_tempo'] += tempo
    genre_features[genre]['total_valence'] += valence

avg_popularity = {genre: genre_popularity[genre] / genre_counts[genre] for genre in genre_popularity}

avg_features = {}
for genre, features in genre_features.items():
    count = genre_counts[genre]
    avg_features[genre] = {
        'avg_danceability': features['total_danceability'] / count,
        'avg_energy': features['total_energy'] / count,
        'avg_loudness': features['total_loudness'] / count,
        'avg_tempo': features['total_tempo'] / count,
        'avg_valence': features['total_valence'] / count
    }

insert_avg_popularity = """
    INSERT INTO golden_avg_popularity_by_genre (track_genre, avg_popularity)
    VALUES (%s, %s)
"""
for genre, avg_pop in avg_popularity.items():
    try:
        session.execute(insert_avg_popularity, (genre, avg_pop))
    except Exception as e:
        print(f"Error inserting average popularity for genre {genre}: {e}")

print("Data inserted into golden_avg_popularity_by_genre.")

insert_feature_averages = """
    INSERT INTO golden_genre_feature_averages (
        track_genre, avg_danceability, avg_energy, avg_loudness, avg_tempo, avg_valence
    ) VALUES (%s, %s, %s, %s, %s, %s)
"""
for genre, features in avg_features.items():
    try:
        session.execute(insert_feature_averages, (
            genre, features['avg_danceability'], features['avg_energy'], 
            features['avg_loudness'], features['avg_tempo'], features['avg_valence']
        ))
    except Exception as e:
        print(f"Error inserting feature averages for genre {genre}: {e}")

print("Data inserted into golden_genre_feature_averages.")

query = "SELECT track_id, artists, album_name, track_name, danceability, energy, loudness FROM silver_tracks;"
rows = session.execute(query)

high_danceability_tracks = []

for row in rows:
    track_id, artists, album_name, track_name, danceability, energy, loudness = row
    if danceability > 0.8:
        high_danceability_tracks.append({
            'track_id': track_id,
            'artists': artists,
            'album_name': album_name,
            'track_name': track_name,
            'danceability': danceability,
            'avg_energy': energy,
            'avg_loudness': loudness
        })

insert_high_danceability = """
    INSERT INTO golden_high_danceability_tracks_aggregated (
        track_id, artists, album_name, track_name, danceability, avg_energy, avg_loudness
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

batch = BatchStatement()
batch_size_limit = 1000
batch_count = 0
inserted_count = 0
total_records = len(high_danceability_tracks)

for track in high_danceability_tracks:
    batch.add(insert_high_danceability, (
        track['track_id'], track['artists'], track['album_name'], track['track_name'], 
        track['danceability'], track['avg_energy'], track['avg_loudness']
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

print("Data inserted successfully into golden_high_danceability_tracks_aggregated.")

count_queries = [
    ("SELECT COUNT(*) FROM golden_avg_popularity_by_genre;", "golden_avg_popularity_by_genre"),
    ("SELECT COUNT(*) FROM golden_genre_feature_averages;", "golden_genre_feature_averages"),
    ("SELECT COUNT(*) FROM golden_high_danceability_tracks_aggregated;", "golden_high_danceability_tracks_aggregated")
]

for query, table_name in count_queries:
    result = session.execute(query)
    for row in result:
        print(f"Total number of records in the {table_name} table: {row[0]}")
