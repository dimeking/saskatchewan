class SqlQueries:
    # insert a songplay records into songplays table
    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT se.ts AS start_time, se.userId AS user_id, sst.song_id AS song_id, sst.artist_id AS artist_id, \
            se.sessionId AS session_id, se.location AS location, se.userAgent AS user_agent
        FROM staging_events_table se
        JOIN staging_songs_table sst
        ON sst.title=se.song AND sst.artist_name=se.artist AND ROUND(sst.duration)=ROUND(se.length) 
        WHERE se.page='NextSong'
    """)

    # insert users into users table from staging_events_table
    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT se.userId AS user_id, se.firstName AS first_name, se.lastName AS last_name, \
            se.gender AS gender, se.level AS level
        FROM staging_events_table se
        WHERE se.page='NextSong'
    """)

    # insert songs into songs table from staging_songs_table
    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT sst.song_id AS song_id, sst.title AS title, sst.artist_id AS artist_id, \
            sst.year AS year, sst.duration AS duration
        FROM staging_songs_table sst
    """)

    # insert artists into artists table from staging_songs_table
    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT sst.artist_id AS artist_id, sst.artist_name AS name, sst.artist_location AS location, \
            sst.artist_latitude AS latitude, sst.artist_longitude AS longitude
        FROM staging_songs_table sst
    """)

    # insert timestamp and associated time attributes into times table
    time_table_insert = ("""
        INSERT INTO times (start_time, dt, hour, day, week, month, year, weekday)
        SELECT 
               DISTINCT se.ts                      AS start_time,
               DATEADD(ms, CAST(se.ts as bigint), '1970-01-01') AS dt,
               EXTRACT(hour FROM dt)               AS hour,
               EXTRACT(day FROM dt)                AS day,
               EXTRACT(week FROM dt)               AS week,
               EXTRACT(month FROM dt)              AS month,
               EXTRACT(year FROM dt)               AS year,
               CASE WHEN EXTRACT(dayofweek FROM dt) IN (6, 7) THEN false ELSE true END AS weekday
        FROM staging_events_table se
        WHERE se.page='NextSong'
    """)