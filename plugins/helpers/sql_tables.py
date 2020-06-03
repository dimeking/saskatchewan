## CREATE TABLES

# store raw data from log_data JSON files on S3 onto staging_events_table 
staging_events_table_create= ("""
DROP TABLE IF EXISTS staging_events_table;
CREATE TABLE IF NOT EXISTS staging_events_table (
    artist varchar, 
    auth varchar, 
    firstName varchar, 
    gender varchar, 
    itemInSession int, 
    lastName varchar,
    length real, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration bigint,
    sessionId bigint,
    song varchar, 
    status int, 
    ts bigint, 
    userAgent varchar(512), 
    userId bigint
) DISTKEY(artist); 
""")

# store raw data from song_data JSON files on S3 onto staging_songs_table 
staging_songs_table_create = ("""
DROP TABLE IF EXISTS staging_songs_table;
CREATE TABLE IF NOT EXISTS staging_songs_table (
    song_id varchar, 
    title varchar(512), 
    num_songs int, 
    year int, 
    duration real, 
    artist_id varchar, 
    artist_name varchar(512), 
    artist_location varchar(512), 
    artist_latitude real, 
    artist_longitude real
) DISTKEY(artist_id) SORTKEY(song_id); 
""")

# records in log data associated with song plays i.e. records with page 'NextSong'
# ensure that a user is using a single session at any particular time
songplay_table_create = ("""
DROP TABLE IF EXISTS songplays;
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(1,1) PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id bigint NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id bigint NOT NULL, 
    location varchar, 
    user_agent varchar(512), 
    UNIQUE (start_time, user_id, session_id)
) DISTKEY(artist_id) SORTKEY(songplay_id);
""")

# users in the app
user_table_create = ("""
{}
CREATE TABLE IF NOT EXISTS {} (
    user_id bigint PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
) SORTKEY(user_id);
""")

# songs in music database
song_table_create = ("""
DROP TABLE IF EXISTS songs;
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar(512), 
    artist_id varchar, 
    year int, 
    duration real
) DISTKEY(artist_id) SORTKEY(song_id);
""")

# artists in music database
artist_table_create = ("""
DROP TABLE IF EXISTS artists;
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar(512), 
    location varchar(512), 
    latitude real, 
    longitude real
) SORTKEY(artist_id);
""")

# timestamps of records in songplays broken down into specific units
time_table_create = ("""
{}
CREATE TABLE IF NOT EXISTS {} (
    start_time bigint PRIMARY KEY, 
    dt DATE NOT NULL, 
    hour smallint NOT NULL, 
    day smallint NOT NULL, 
    week smallint NOT NULL, 
    month smallint NOT NULL, 
    year smallint NOT NULL, 
    weekday boolean NOT NULL
) SORTKEY(start_time);
""")