import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    - reads a song file and extract its data 
    - prepares required data for song and artist tables
    - insert corresponding records into each table

    Args:
        cur ([object]): [Allows Python code to execute PostgreSQL command in a database session.
            Cursors are created by the connection.cursor()] 
        filepath ([string]): [Absolute path to the target song file]
    """

    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data) 
    
    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath, bulk_insert=False):
    """
    - reads a log file and extract its data 
    - prepares required data for time and users and songplays tables
    - insert corresponding records into each table

    Args:
        cur ([object]): [Allows Python code to execute PostgreSQL command in a database session.
            Cursors are created by the connection.cursor()] 
        filepath ([string]): [Absolute path to the target log file]
        bulk_insert ([boolean]): [allows bulk insert of data into a table]
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']
    df = df.reset_index(drop=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_df = pd.DataFrame()
    time_df['start_time'] = df.ts
    time_df['hour'] = t.dt.hour
    time_df['day'] = t.dt.day
    time_df['weekofyear'] = t.dt.weekofyear
    time_df['month'] = t.dt.month
    time_df['year'] = t.dt.year
    time_df['weekday'] = t.dt.weekday
 
    if bulk_insert:
        bulk_insert_from_csv(df=time_df, cursor=cur, table_name='time')
    else:
        ## isert per row
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))    

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    if bulk_insert:
        bulk_insert_from_csv(df=user_df, cursor=cur, table_name='users')
    else:
        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - finds out all files matching extension from a given directory
    - applys the give process function on each file 

    Args:
        cur ([object]): [Allows Python code to execute PostgreSQL command in a database session.
            Cursors are created by the connection.cursor()]
        conn ([object]): [used to connect to default database]
        filepath ([string]): [Absolute path to the target log file]
        func ([type]): [Process function to be applied on each data file]
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - creates connection and cursor to the sparkifydb
    - joins songs and artists table for further usage (denormalization)
    - applys process functions on sparkify data
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    # join songs and artists tables for filtering songs (denormalization)
    cur.execute(temp_song_artist)
    
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()