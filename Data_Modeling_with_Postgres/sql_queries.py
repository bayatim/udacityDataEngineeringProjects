# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id SERIAL PRIMARY KEY,
                        start_time bigint references time(start_time),
                        user_id int references users(user_id),
                        level VARCHAR(255),
                        song_id VARCHAR(255) references songs(song_id),
                        artist_id VARCHAR(255) references artists(artist_id),
                        session_id int,
                        location VARCHAR(255),
                        user_agent VARCHAR(255)
                        )
                        """)

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                    user_id int PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    gender VARCHAR(255),
                    level VARCHAR(255)
                    )
                    """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(255) PRIMARY KEY,
                    title VARCHAR(255),
                    artist_id VARCHAR(255) references artists(artist_id),
                    year int,
                    duration float
                    )
                    """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                      artist_id VARCHAR(255) PRIMARY KEY,
                      name VARCHAR(255),
                      location VARCHAR(255),
                      latitude float,
                      longitude float
                      )
                      """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                    start_time bigint PRIMARY KEY,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday int
                    )
                    """)

# INSERT RECORDS

songplay_table_insert = """ INSERT INTO songplays (start_time, user_id,
                        level, song_id, artist_id, session_id, location, user_agent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """

user_table_insert = """ INSERT INTO users (user_id, first_name, last_name, gender, level)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET level = excluded.level
                    """

song_table_insert = """ INSERT INTO songs  (song_id, title, artist_id, year, duration)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id) DO NOTHING """

artist_table_insert = """ INSERT INTO artists  (artist_id, name, location, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id) DO NOTHING """


time_table_insert = """ INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (start_time) DO NOTHING
                    """

# bulk insert
def bulk_insert_from_csv(df, cursor, table_name: str):
    """
    - insert (csv) file records into a given table

    Args:
        df ([object]): [pandas dataframe which includes data records] 
        cursor ([object]): [Allows Python code to execute PostgreSQL command in a database session.
            Cursors are created by the connection.cursor()]
        table_name ([string]): [name of the table to be inserted data into]
    """
    import io
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=True, index=False)
    output.seek(0) # Required for rewinding the String object
    query = f"COPY {table_name} FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
    cursor.copy_expert(query, output)
     


# FIND SONGS

temp_song_artist = """
                   DROP TABLE IF EXISTS temp_song_artist;
                   SELECT songs.song_id as song_id, songs.title as title, songs.duration as duration,
                   artists.name as name, artists.artist_id as artist_id
                   INTO temp_song_artist
                   FROM songs
                   JOIN artists
                   ON songs.artist_id = artists.artist_id
                   """

song_select = (""" SELECT song_id, artist_id
                   FROM temp_song_artist
                   WHERE title = %s and name = %s and duration = %s
               """)


# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]