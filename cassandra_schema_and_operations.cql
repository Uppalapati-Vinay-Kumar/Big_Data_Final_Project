-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS spotify;

-- Switch to the keyspace
USE spotify;

-- Bronze Level Table (Raw Track Data)
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

-- Silver Level Table (Refined Track Data)
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

-- Gold Level Table 1: Average Popularity by Genre
CREATE TABLE IF NOT EXISTS golden_avg_popularity_by_genre (
    track_genre TEXT PRIMARY KEY,
    avg_popularity FLOAT
);

-- Gold Level Table 2: Genre Feature Averages
CREATE TABLE IF NOT EXISTS golden_genre_feature_averages (
    track_genre TEXT PRIMARY KEY,
    avg_danceability FLOAT,
    avg_energy FLOAT,
    avg_loudness FLOAT,
    avg_tempo FLOAT,
    avg_valence FLOAT
);

-- Gold Level Table 3: High Danceability Tracks with Aggregated Features
CREATE TABLE IF NOT EXISTS golden_high_danceability_tracks_aggregated (
    track_id TEXT PRIMARY KEY,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    danceability FLOAT,
    avg_energy FLOAT,
    avg_loudness FLOAT
);

-- Insert Data into Bronze Level Table
INSERT INTO bronze_tracks (
    unique_id, track_id, artists, album_name, track_name, popularity, duration_ms, 
    explicit, danceability, energy, key, loudness, mode, speechiness, acousticness, 
    instrumentalness, liveness, valence, tempo, time_signature, track_genre
) VALUES (
    uuid(), 'track_id_value', 'artist_name_value', 'album_name_value', 'track_name_value', 
    85, 240000, true, 0.85, 0.90, 5, -5.0, 1, 0.10, 0.50, 0.20, 0.40, 0.60, 120.0, 4, 'rock'
);

-- Insert Data into Silver Level Table
INSERT INTO silver_tracks (
    unique_id, track_id, artists, album_name, track_name, popularity, duration_ms, 
    explicit, danceability, energy, key, loudness, mode, speechiness, acousticness, 
    liveness, valence, tempo, time_signature, track_genre
) VALUES (
    uuid(), 'track_id_value', 'artist_name_value', 'album_name_value', 'track_name_value', 
    90, 220000, 1, 0.80, 0.85, 3, -4.5, 0, 0.08, 0.60, 0.30, 0.35, 0.55, 115.0, 4, 'pop'
);

-- Insert Data into Gold Level Table 1
INSERT INTO golden_avg_popularity_by_genre (
    track_genre, avg_popularity
) VALUES (
    'rock', 75.5
);

-- Insert Data into Gold Level Table 2
INSERT INTO golden_genre_feature_averages (
    track_genre, avg_danceability, avg_energy, avg_loudness, avg_tempo, avg_valence
) VALUES (
    'rock', 0.75, 0.80, -4.0, 120.0, 0.70
);

-- Insert Data into Gold Level Table 3
INSERT INTO golden_high_danceability_tracks_aggregated (
    track_id, artists, album_name, track_name, danceability, avg_energy, avg_loudness
) VALUES (
    'track_id_value', 'artist_name_value', 'album_name_value', 'track_name_value', 
    0.95, 0.85, -3.5
);

-- Query all data from bronze_tracks
SELECT * FROM bronze_tracks;

-- Query all data from silver_tracks
SELECT * FROM silver_tracks;

-- Query specific columns from silver_tracks for track_genre_prediction_model
SELECT danceability, energy, key, loudness, mode, 
       speechiness, acousticness, liveness, valence, tempo, time_signature, popularity
FROM silver_tracks;

-- Query specific columns from silver_tracks for track_popularity_prediction_model
SELECT danceability, energy, key, loudness, mode, 
       speechiness, acousticness, liveness, valence, tempo, time_signature, track_genre
FROM silver_tracks;

-- Query specific columns from silver_tracks to insert data gold level tables
SELECT track_genre, track_id, popularity, danceability, energy, loudness, tempo, valence, artists, album_name, track_name FROM silver_tracks;

-- Query the average popularity by genre
SELECT * FROM golden_avg_popularity_by_genre;

-- Query the average feature values by genre
SELECT * FROM golden_genre_feature_averages;

-- Query all high danceability tracks
SELECT * FROM golden_high_danceability_tracks_aggregated;






