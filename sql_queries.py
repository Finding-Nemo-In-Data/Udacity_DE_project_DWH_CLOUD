#!/usr/bin/env python
# coding: utf-8

# In[5]:


import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
                                    events_id INT IDENTITY(0,1) PRIMARY KEY,
                                    artist VARCHAR (MAX),
                                    auth VARCHAR (50),
                                    first_name VARCHAR (50),
                                    gender CHAR(2),
                                    item_in_session INT,
                                    last_name VARCHAR (50),
                                    length FLOAT8,
                                    level VARCHAR (20),
                                    location VARCHAR(255),
                                    method CHAR(5),
                                    page VARCHAR(50),
                                    registration FLOAT8,
                                    session_id BIGINT,
                                    song VARCHAR (255),
                                    status INT,
                                    ts BIGINT,
                                    user_agent VARCHAR (255),
                                    user_id VARCHAR (255)
                                    );
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs (
                                    song_id VARCHAR(255) PRIMARY KEY, 
                                    num_songs INT,
                                    artist_id VARCHAR (50),
                                    artist_lattitude FLOAT8,
                                    artist_longitude FLOAT8,
                                    artist_location VARCHAR (MAX),
                                    artist_name VARCHAR (MAX),
                                    title VARCHAR (MAX),
                                    duration FLOAT8,
                                    year INT
                                    );
""")


songplay_table_create = (""" CREATE TABLE songplays (
                                songplay_id INT IDENTITY (0,1) PRIMARY KEY,
                                start_time BIGINT,
                                user_id VARCHAR (255),
                                level VARCHAR (20),
                                song_id VARCHAR (255),
                                artist_id VARCHAR (50),
                                session_id BIGINT,
                                location VARCHAR(255),
                                user_agent VARCHAR(255));
""")

user_table_create = (""" CREATE TABLE users (
                            user_id VARCHAR (255) PRIMARY KEY,
                            level VARCHAR (20),
                            first_name VARCHAR (50),
                            last_name VARCHAR (50),
                            gender VARCHAR (5)
                            );
""")

song_table_create = (""" CREATE TABLE songs (
                            song_id VARCHAR (255) PRIMARY KEY,
                            title VARCHAR (MAX),
                            artist_id VARCHAR (50),
                            year INT,
                            duration FLOAT8);
""")

artist_table_create = (""" CREATE TABLE artists (
                                artist_id VARCHAR (50) PRIMARY KEY,
                                artist_name VARCHAR (MAX),
                                location VARCHAR (MAX),
                                lattitude FLOAT8,
                                longitude FLOAT8);
""")

time_table_create = (""" CREATE TABLE time (
                            start_time TIMESTAMP PRIMARY KEY,
                            hour INT,
                            day INT,
                            week INT,
                            month INT,
                            year INT,
                            weekday VARCHAR (25));
""")


# STAGING TABLES

staging_events_copy = (""" COPY staging_events FROM '{}'
                            CREDENTIALS 'aws_iam_role={}'
                            REGION 'us-west-2'
                            COMPUPDATE OFF STATUPDATE OFF
                            JSON '{}';
                            """).format(config.get('S3', 'LOG_DATA'),
                                        config.get('IAM_ROLE', 'ARN'),
                                        config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (""" COPY staging_songs FROM '{}'
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            JSON 'auto' ;
                            """).format(config.get ('S3','SONG_DATA'), 
                                        config.get ('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        e.ts, 
        e.user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location,
        e.user_agent
    FROM staging_events e
    JOIN staging_songs s
    ON e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = (""" INSERT INTO users (user_id, last_name, gender, level, first_name)
    SELECT DISTINCT 
        user_id,
        last_name,
        gender,
        level,
        first_name
    FROM staging_events
    WHERE page='NextSong';
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = (""" INSERT INTO artists(artist_id,artist_name, location, lattitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_lattitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        EXTRACT(hr from start_time) as hour,
        EXTRACT(d from start_time) as day,
        EXTRACT(w from start_time) as week,
        EXTRACT(mon from start_time) as month,
        EXTRACT(yr from start_time) as year,
        EXTRACT(weekday from start_time) AS weekday
    FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
    FROM staging_events
    )
    ; 
        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
                                       
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,song_table_drop, artist_table_drop, time_table_drop]
                                        
copy_table_queries = [staging_events_copy, staging_songs_copy]
                                        
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# In[ ]:




