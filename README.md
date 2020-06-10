# Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal is to create a database schema, deploy the schema on Postgres and implement ETL piple to populate data in the tables. The results can be validated by running queries provided by the analytics team from Sparkify.

## Dataset

### First Dataset -> Song Dataset

Song data is stored in a JSON format. Each file contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

### Second Dataset -> Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.


## Schema for Song Play Analysis

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
`songplay_id (serial) (PK), start_time (FK - TIME), user_id (FK - USERS), level, song_id, artist_id, session_id, location, user_agent`

### Dimension Tables

users - users in the application
`user_id INT (PK), first_name , last_name  , gender, level`

songs - songs in music database
`song_id (PK), title, artist_id (FK - ARTISTS), year, duration`

artists - artists in music database
`artist_id, name, location, latitude, longitude`

time - timestamps of records in songplays broken down into specific units
`start_time, hour, day, week, month, year, weekday`


## Scripts


### Initialization py Scripts - 

create_tables.py drops and initializes the database `sparkify`  ***Please run this with caution!***

sql_queries.py contains all sql queries necessary to create the database, create tables, and insert data. It is imported into create_tables and etl scripts.

### ETL py Script -
etl.py reads and processes files from song_data and log_data and loads them into the data model.
