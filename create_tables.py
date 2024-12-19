"""
Creates and manages Cassandra tables for track data at various levels (Bronze, Silver, and Gold).

1. **Bronze**: Stores raw track data.
2. **Silver**: Stores refined track data.
3. **Gold Level 1**: Stores average popularity by genre.
4. **Gold Level 2**: Stores genre feature averages.
5. **Gold Level 3**: Stores high danceability tracks with aggregated features.

Uses `get_session()` from `cassandra_connection` to interact with the database.
"""

from cassandra_connection import get_session

session = get_session()

# Create Bronze Level Table
session.execute(""" 
    CREATE TABLE IF NOT EXISTS bronze_tracks (
        unique_id UUID PRIMARY KEY,
        track_id TEXT,
        artists TEXT,
        album_name TEXT,
        track_name TEXT,
        popularity INT,
        duration_ms INT,
        explicit BOOLEAN,
        danceability FLOAT,
        energy FLOAT,
        key INT,
        loudness FLOAT,
        mode INT,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        time_signature INT,
        track_genre TEXT
    );
""")
print("Bronze Level Tracks Table created successfully.")


# Create Silver Level Table
session.execute("""
    CREATE TABLE IF NOT EXISTS silver_tracks (
    unique_id UUID PRIMARY KEY,
    track_id TEXT,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity INT,
    duration_ms INT,
    explicit INT,
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    track_genre TEXT
    );
""")
print("Silver Level Tracks Table created successfully.")

# Create Gold Level Table 1: average popularity
session.execute("""
    CREATE TABLE IF NOT EXISTS golden_avg_popularity_by_genre (
    track_genre TEXT PRIMARY KEY,
    avg_popularity FLOAT
    );
""")
print("Gold Level Table (average popularity) created successfully.")

# Create Gold Level Table 2: genre feature averages
session.execute("""
    CREATE TABLE IF NOT EXISTS golden_genre_feature_averages (
    track_genre TEXT PRIMARY KEY,
    avg_danceability FLOAT,
    avg_energy FLOAT,
    avg_loudness FLOAT,
    avg_tempo FLOAT,
    avg_valence FLOAT
    );
""")
print("Gold Level Table (genre feature averages) created successfully.")

# Create Gold Level Table 3: high danceability tracks
session.execute("""
    CREATE TABLE IF NOT EXISTS golden_high_danceability_tracks_aggregated (
    track_id TEXT PRIMARY KEY,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    danceability FLOAT,
    avg_energy FLOAT,
    avg_loudness FLOAT
    );
""")
print("Gold Level Table (high danceability tracks) created successfully.")