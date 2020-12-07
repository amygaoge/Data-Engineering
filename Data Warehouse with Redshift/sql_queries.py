import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA= config.get("S3","LOG_DATA")
LOG_JSONPATH= config.get("S3","LOG_JSONPATH")
SONG_DATA= config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN").strip("'")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
iteminSession VARCHAR,
lastName VARCHAR,
length DECIMAL,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionid INTEGER,
song VARCHAR,
status INTEGER ,
ts BIGINT,
userAgent VARCHAR,
userid INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id INTEGER IDENTITY (0, 1) PRIMARY KEY,
time_start_time BIGINT NOT NULL SORTKEY,
user_id VARCHAR NOT NULL,
song_id VARCHAR DISTKEY,
artist_id VARCHAR,
songplay_session_id INT,
songplay_location VARCHAR,
songplay_user_agent VARCHAR
); 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id VARCHAR PRIMARY KEY SORTKEY, 
user_first_name VARCHAR, 
user_last_name VARCHAR, 
user_gender VARCHAR, 
user_level VARCHAR
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR PRIMARY KEY DISTKEY SORTKEY, 
song_title VARCHAR, 
song_duration NUMERIC, 
song_year INT,
artist_id VARCHAR NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR PRIMARY KEY SORTKEY, 
artist_name VARCHAR, 
artist_location VARCHAR, 
artist_latitude NUMERIC, 
artist_longitude NUMERIC
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
time_start_time BIGINT PRIMARY KEY SORTKEY,
time_hour int, 
time_day int, 
time_week int, 
time_month int, 
time_year int, 
time_weekday int
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
            COPY staging_events
            FROM {}
            CREDENTIALS 'aws_iam_role={}'
            FORMAT AS json {}
            REGION 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
            COPY staging_songs
            FROM {}
            CREDENTIALS 'aws_iam_role={}'
            FORMAT AS json 'auto'
            REGION 'us-west-2';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(time_start_time,
user_id,
song_id,
artist_id,
songplay_session_id,
songplay_location,
songplay_user_agent)
SELECT DISTINCT
se.ts AS time_start_time,
se.userid AS user_id,
ss.song_id AS song_id,
ss.artist_id AS artist_id,
se.sessionid AS songplay_session_id,
se.location AS songplay_location,
se.userAgent AS songplay_user_agent
FROM staging_events se
LEFT JOIN staging_songs ss
ON se.song = ss.title and se.artist = ss.artist_name and se.length = ss.duration
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
(
user_id, 
user_first_name, 
user_last_name, 
user_gender,
user_level
 )
SELECT DISTINCT
se.userid AS user_id,
se.firstName AS user_first_name,
se.lastName AS user_last_name,
se.gender AS user_gender,
se.level AS user_level
FROM staging_events se
WHERE se.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO Songs 
(
song_id,
song_title, 
song_duration, 
song_year,
artist_id)
SELECT DISTINCT
ss.song_id AS song_id,
ss.title AS song_title,
ss.duration AS song_duration,
ss.year AS song_year,
ss.artist_id AS artist_id
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists
(
artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
)
SELECT DISTINCT 
ss.artist_id AS artist_id,
ss.artist_name AS artist_name,
ss.artist_location AS artist_location,
ss.artist_latitude AS artist_latitude,
ss.artist_longitude AS artist_longitude
FROM staging_songs ss
""")


time_table_insert = ("""
INSERT INTO time 
(
time_start_time,
time_hour,
time_day,
time_week,
time_month,
time_year,
time_weekday)
SELECT DISTINCT
s.time_start_time AS time_start_time,
EXTRACT (hour from TIMESTAMP 'epoch' + s.time_start_time/1000 * INTERVAL '1 second') AS time_hour,
EXTRACT (day from TIMESTAMP 'epoch' + s.time_start_time/1000 * INTERVAL '1 second') AS time_day,
EXTRACT (week from TIMESTAMP 'epoch' + s.time_start_time/1000 * INTERVAL '1 second') AS time_week,
EXTRACT (month from TIMESTAMP 'epoch' + s.time_start_time/1000 * INTERVAL '1 second') AS time_month,
EXTRACT (year from TIMESTAMP 'epoch' + s.time_start_time/1000 * INTERVAL '1 second') AS time_year,
EXTRACT (weekday from TIMESTAMP 'epoch' + s.time_start_time/1000 * INTERVAL '1 second') AS time_weekday
FROM songplays s
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy , staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]