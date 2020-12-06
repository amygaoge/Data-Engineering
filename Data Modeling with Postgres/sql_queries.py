# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id BIGSERIAL, time_start_time BIGINT NOT NULL, user_id INT NOT NULL, song_id INT, artist_id VARCHAR, songplay_session_id INT, songplay_location VARCHAR, songplay_user_agent VARCHAR, PRIMARY KEY(songplay_id)); 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id VARCHAR, user_first_name VARCHAR, user_last_name VARCHAR, user_gender VARCHAR, user_level VARCHAR, PRIMARY KEY(user_id));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR, song_title VARCHAR, song_duration NUMERIC, song_year INT, artist_id VARCHAR, PRIMARY KEY(song_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR, artist_name VARCHAR, artist_location VARCHAR, artist_latitude NUMERIC, artist_longitude NUMERIC, PRIMARY KEY(artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (time_start_time BIGINT, time_hour int, time_day int, time_week int, time_month int, time_year int, time_weekday int, PRIMARY KEY(time_start_time));
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (time_start_time, user_id, song_id, artist_id, songplay_session_id, songplay_location, songplay_user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
INSERT INTO users (user_id, user_first_name, user_last_name, user_gender, user_level) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT(user_id) DO UPDATE
SET user_level = EXCLUDED.user_level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, song_title, song_duration, song_year, artist_id) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT(artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (time_start_time, time_hour, time_day, time_week, time_month, time_year, time_weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(time_start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id 
FROM songs s
LEFT JOIN artists a ON s.artist_id = a.artist_id
WHERE a.artist_name = %s
and s.song_title = %s
and s.song_duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]