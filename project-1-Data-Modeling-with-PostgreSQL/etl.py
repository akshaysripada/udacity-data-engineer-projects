import os
import glob
import psycopg2
import pandas as pd
import json
from sql_queries import *


def process_song_file(cur, filepath):
    """Read every song data file and load data into song and artist table"""
    # open song file
    df = pd.read_json(filepath, lines=True)
    # replace NaN values to None, this will be loaded as NULL in sql
    df = df.where(pd.notnull(df), None)
    
    
    # column_name contains all the column headers for the song table
    column_name = ['song_id', 'title', 'artist_id', 'year', 'duration']
    # Using the colum_name list we extract the corresponding song column data from the panda dataframe
    song_data = tuple(df[column_name].values[0])
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # column_name contains all the column headers for the artist table
    column_name = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    # Using the colum_name list we extract the corresponding artist column data from the panda dataframe
    artist_data = tuple(df[column_name].values[0])
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Read every log data file and load the batch of data into user, time and songplay table"""
    
    # open log file
    df = pd.read_json(filepath, lines=True)
    # replace NaN values to None, this will be loaded as NULL in sql
    df = df.where(pd.notnull(df), None)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # load a list of time data from the time dataframe. dt property from pandas is used to convert the timestamp into the required units
    time_data = [list(t), list(t.dt.hour), list(t.dt.day), list(t.dt.week), list(t.dt.month), list(t.dt.year), list(t.dt.weekday)]
    
    # column_name contains all the column headers for the time table
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # time_dictionary is a dictionary with column names as the keys  linked to the corresponding values from time_data list
    time_dictionary = dict(zip(column_labels, time_data))
    
    # using the dictionary created above to create a time panda dataframe that can be iterated through to create the needed time records
    time_df = pd.DataFrame.from_dict(time_dictionary) 

    # insert time data records by looping through the time dataframe
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    
    # filter the required user data columns from the log data panda dataframe. 
    user_df = df.filter(['userId', 'firstName', 'lastName', 'gender', 'level'], axis=1)
    
    # droping duplicate records from the dataframe, this will decrease the number of redundant insert statements for the user table
    user_df = user_df.drop_duplicates() 

    # insert user data records by looping through the user dataframe
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

        # insert songplay record if we found a song record in the song_select query
        if songid is not None:
            # create a list of data for the columns in the songplay table
            # all the data except songid and artsitid comes from the log data panda dataframe
            # the songid and artistid is fetched in the song_select query above
            songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Gather all the song or log data files. Iterate through the list of files and call the process function for each file"""
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()