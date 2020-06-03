import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''processes song file:
    - reads song json file 
    - select columns 'song_id', 'title', 'artist_id' ,'year', 'duration' and populates song table. 
    - select columns 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' and populates artist table. 
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # select columns needed for songs table.
    song_data = df[['song_id', 'title', 'artist_id' ,'year', 'duration']].values[0].tolist()
    
    try:
        # insert song record
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print (e)
    
    # select columns needed for artist table.
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()    
    
    try:
        # insert artist record
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print (e)

def process_log_file(cur, filepath):
    '''processes log file:
    - reads log json file
    - filters the data only for page = NextSong
    - extracts columns 'timestamp', 'hour','day', 'week of year', 'month', 'year', 'weekday' and populates time table. 
    - extracts columns 'userId', 'firstName', 'lastName', 'gender', 'level' and populates user table.
    - extracts columns start_time, user_id, level , song_id , artist_id , session_id , location , user_agent and populates songplays table. 
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = [df['ts'].values.tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), \
             t.dt.weekofyear.tolist(), t.dt.month.tolist(), t.dt.year.tolist(), \
             t.dt.weekday.tolist()]
    
    column_labels = ['timestamp', 'hour','day', 'week of year', 'month', 'year', 'weekday']
    
    time_df = pd.DataFrame(time_data).transpose()
    time_df.columns = column_labels

    for i, row in time_df.iterrows():
        try:
            # insert time data records
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)

    # load user table
    
    # select columns needed for user table.
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    for i, row in user_df.iterrows():
        try:
            # insert user records
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)
    
    # Load songplays table
    
    songplay_data = []
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        # if both songid and artistid are populated then assign the values otherwise change value to None,None
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # append records to songplay_data list of tuples. This list will be later used to bulk insert in songplays table. 
        item = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        songplay_data.append(item)

    # create the df of songplay_list and save it as temp csv for bulk insert. 
    songplay_df = pd.DataFrame(songplay_data,columns = ['ts' , 'userId', 'level' , 'songid', 'artistid', 'sessionId', 'location', 'userAgent'])

    songplay_df.to_csv('test_songplay_df.csv', index=False, header=False)
    
    # Create the copy command for songplays table. 
    sqlstr = "COPY songplays (start_time, user_id, level , song_id , artist_id , session_id , location , user_agent) FROM STDIN DELIMITER ',' CSV"
    
    # Open csv file to bulk insert using copy command. 
    with open('test_songplay_df.csv') as f:
        cur.copy_expert(sqlstr, f)
    
    # delete the csv file. 
    os.remove('test_songplay_df.csv')

def process_data(cur, conn, filepath, func):
    '''
    function to start the data processing steps.
    - Get all the files 
    - Iterate over the files based on the func passed. 
    '''
    
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
    '''
    Main function:
    1. Setup connection 
    2. Initiate data processing for song_data then log_data by calling process_data and passing 
       process_song_file and process_log_file functions respectively. 
    '''
    
    # Connect to server
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Connecting to db sparkifydb")
        print (e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    try:
        conn.close()
    except psycopg2.Error as e: 
        print("Error: Closing the connection")
        print (e)


if __name__ == "__main__":
    main()