import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('DWH', 'DWH_ROLE_ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create = """ CREATE TABLE IF NOT EXISTS staging_events (
                                s_events_id int IDENTITY(0,1),
                                artist VARCHAR(255),
                                auth VARCHAR(255),
                                firstName VARCHAR(255),
                                gender VARCHAR(255),
                                itemInSession VARCHAR(255),
                                lastName VARCHAR(255),
                                length VARCHAR(255),
                                level VARCHAR(255),
                                location VARCHAR(255),
                                method VARCHAR(255),
                                page VARCHAR(255),
                                registration VARCHAR(255),
                                sessionId VARCHAR(255),
                                song VARCHAR(255),
                                status VARCHAR(255),
                                ts VARCHAR(255),
                                userAgent VARCHAR(255),
                                userId VARCHAR(255),
                                PRIMARY KEY (s_events_id)
                                )
                                """

staging_songs_table_create = """ CREATE TABLE IF NOT EXISTS staging_songs (
                                s_songs_id int IDENTITY(0,1),
                                num_songs VARCHAR(255),
                                artist_id VARCHAR(255),
                                artist_latitude VARCHAR(255),
                                artist_longitude VARCHAR(255),
                                artist_location VARCHAR(255),
                                artist_name VARCHAR(255),
                                song_id VARCHAR(255),
                                title VARCHAR(255),
                                duration VARCHAR(255),
                                year VARCHAR(255),
                                PRIMARY KEY (s_songs_id)
                                )
                                """


songplay_table_create = """ CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id int IDENTITY(0, 1),
                        start_time bigint references time(start_time),
                        user_id int references users(user_id),
                        level VARCHAR(255),
                        song_id VARCHAR(255) references songs(song_id),
                        artist_id VARCHAR(255) references artists(artist_id),
                        session_id int,
                        location VARCHAR(255),
                        user_agent VARCHAR(255),
                        PRIMARY KEY(songplay_id)
                        )
                        """

user_table_create = """ CREATE TABLE IF NOT EXISTS users (
                    user_id int UNIQUE NOT NULL,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    gender VARCHAR(255),
                    level VARCHAR(255),
                    PRIMARY KEY (user_id)
                    )
                    """

song_table_create = """ CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(255),
                    artist_id VARCHAR(255) references artists(artist_id),
                    year int,
                    duration float,
                    PRIMARY KEY (song_id)
                    )
                    """

artist_table_create = """ CREATE TABLE IF NOT EXISTS artists (
                      artist_id VARCHAR(255) UNIQUE NOT NULL,
                      name VARCHAR(255),
                      location VARCHAR(255),
                      latitude numeric(8, 6),
                      longitude numeric(9, 6),
                      PRIMARY KEY (artist_id)
                      )
                      """

time_table_create = """ CREATE TABLE IF NOT EXISTS time (
                    start_time bigint UNIQUE NOT NULL,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday int,
                    PRIMARY KEY (start_time)
                    )
                    """

# STAGING TABLES

staging_events_copy= """COPY staging_events from 's3://udacity-dend/log_data' 
                    credentials 'aws_iam_role={}'
                    json 's3://udacity-dend/log_json_path.json'
                    region 'us-west-2';
                    """.format(DWH_ROLE_ARN)

staging_songs_copy= """COPY staging_songs from 's3://udacity-dend/song_data' 
                    credentials 'aws_iam_role={}'
                    json 'auto'
                    region 'us-west-2';
                    """.format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
                        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        SELECT  se.ts::bigint  AS start_time,
                                se.userid::int AS user_id,
                                se.level,
                                ss.song_id,
                                ss.artist_id,
                                se.sessionid::int   AS session_id,
                                se.location,
                                se.useragent
                        FROM staging_events AS se
                        JOIN staging_songs AS ss
                        ON se.artist = ss.artist_name
                        WHERE se.page = 'NextSong'
                        """

user_table_insert = """
                    INSERT INTO users (user_id, first_name, last_name, gender, level)
                    SELECT  DISTINCT(se.userid)::int AS user_id,
                            se.firstname             AS first_name,
                            se.lastname              AS last_name,
                            se.gender,
                            se.level
                    FROM staging_events AS se
                    RIGHT JOIN 
                    (
                        SELECT  userid,
                                MAX(ts) AS ts
                        FROM staging_events
                        WHERE userid is not NULL AND page = 'NextSong'
                        GROUP BY userid
                    ) AS sem
                    ON se.userid = sem.userid AND se.ts = sem.ts
                    WHERE se.userid is not NULL AND se.page = 'NextSong'
                    """

song_table_insert = """
                    INSERT INTO songs  (song_id, title, artist_id, year, duration)
                    SELECT  DISTINCT(song_id),
                            title,
                            artist_id,
                            year::int,
                            duration::float
                    FROM staging_songs
                    """

artist_table_insert = """    
                            INSERT INTO artists  (artist_id, name, location, latitude, longitude)
                             SELECT  DISTINCT(artist_id) AS artist_id,
                                     artist_name         AS name,
                                     artist_location     AS location,
                                     artist_latitude     AS latitude,
                                     artist_longitude    AS longitude
                             FROM staging_songs
                             """

time_table_insert = """ INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        WITH tsd AS (SELECT ts, TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as tstamp
                        FROM staging_events
                        WHERE page = 'NextSong'
                        )
                        SELECT  DISTINCT(tsd.ts::bigint) as start_time,
                                EXTRACT(hour FROM tsd.tstamp)::int as hour,
                                EXTRACT(day FROM tsd.tstamp)::int as day,
                                EXTRACT(week FROM tsd.tstamp)::int as week,
                                EXTRACT(month FROM tsd.tstamp)::int as month,
                                EXTRACT(year FROM tsd.tstamp)::int as year,
                                EXTRACT(weekday FROM tsd.tstamp)::int as weekday
                        FROM tsd
                    """

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



