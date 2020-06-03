# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop =  "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
                ( songplay_id SERIAL, 
                start_time bigint, 
                user_id VARCHAR, 
                level char(4), 
                song_id VARCHAR, 
                artist_id VARCHAR, 
                session_id int, 
                location varchar, 
                user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
                 ( user_id INT PRIMARY KEY, 
                    first_name VARCHAR , 
                    last_name VARCHAR , 
                    gender char(1), 
                    level char(4));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
                (song_id VARCHAR PRIMARY KEY, 
                title VARCHAR, 
                artist_id VARCHAR, 
                year INT, 
                duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
                (artist_id VARCHAR PRIMARY KEY, 
                name VARCHAR, 
                location VARCHAR, 
                latitude FLOAT, 
                longitude FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
                (start_time bigint PRIMARY KEY, 
                hour int, 
                day int , 
                week int, 
                month int, 
                year int, 
                weekday VARCHAR);
""")

# INSERT RECORDS
# songplay_id, 
songplay_table_insert = ("""
INSERT INTO songplays
(
start_time, 
user_id, 
level , 
song_id , 
artist_id , 
session_id , 
location , 
user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
insert into users 
(
user_id, 
first_name, 
last_name, 
gender, 
level)
values (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO NOTHING
""")

song_table_insert = ("""
INSERT INTO songs 
                (song_id, 
                title, 
                artist_id, 
                year, 
                duration)
values (%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists 
                (artist_id, 
                name, 
                location, 
                latitude, 
                longitude)
values (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
insert into time
(start_time, 
                hour, 
                day, 
                week, 
                month, 
                year, 
                weekday)
values (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT a.artist_id, s.song_id 
FROM songs s
inner join artists a
on s.artist_id = a.artist_id
where s.title = %s
and a.name = %s
and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]