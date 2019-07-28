# Data Warehouse

## Script description
The Data Warehouse project uses three scripts to create the staging and final tables and load the song and log data into the databse. The three scripts are described below:
1. sql_queries.py: This script contains all the sql querries used by the other two scripts. It is broken down into drop tables, create tables, load staging records and insert records into fact and dimension tables. 
2. create_tables.py: This script first drops tables if they are already exist on the database. The script then creates tables using the commands specified in sql_queries.
3. etl.py: This script loads data from the song and log data files into the tables. load_staging_tables uses the copy command to load event and song data from S3 into the Redshift databse. insert_tables then inserts records into the fact and dimension tables using the data loaded onto the staging tables. 

The create_tables.py must be run first to drop any existant tables and then create the tables. The etl.py must be run next to process the data from song and log files and then insert the data into the tables in the sparkifydb database.

## Database context for Sparkify
This database will be critical for analytics for the start up, Sparkify. The songs and artists table track all the data in the song library. The time and users tabels track when the individual user has logged into a session. The combination of the data in these tables would provide user's listening or song playing information. The songplays fact table used to query out user's listening activity. This data can play a critical role in shaping the business decisions at the start up.

## Schema design
The schema has two types of tables. The first part is just for staging data from the log and song data provided on S3:
1. staging_events: The staging_events is the staging table for the log data. It holds all the information about the songplays on the application. This forms the source for users, time and songplay tables.
2. staging_songs: The staging_songs is the staging table for the song data. This forms the source for song and artist tables. The song_id and artist_id column in songplay needs to also be populated using this table.

The second part is fact and dimension table for the data warehouse that holds the information for analytical querying:
1. Songs: The song table has song_id as its varchar primary key and supporting columns such as song title, year of release and duration of the song. The table also has a artist_id column that links every song to the artist table. The two tables can be joined to query information such as, what are all the songs released by one artist and so on. The song information comes from song data files, and thus the insert statement will have to deal with duplicates. Thus the insert statement for songs from song data staging tables uses the distinct clause to only insert unique songs.
2. Artists: The artist table has artist_id as its varchar primary key and supporting columns such as artist name, location, lattitude and longitude information. The location, longitude and latitude columns are nullable as some artists locations are unknown. The artist information comes from song data files, and thus the insert statement will have to deal with duplicates since a single artists does release multiple songs. Thus the insert statement for artists from song data staging tables uses the distinct clause to only insert unique artists.  
3. Users: The users table has user_id as its integer primary key and supporting varchar columns such as first name, last name, gender and level. The user_id can be used as the primary_key to avoid duplicate users in the table. To make sure we only create one record per user the insert statement from log data needs to handle duplicates. The insert statement has a subquery that picks the most recent timestamp in the log data for every user_id. This information from the subquery is then used to get the most up to date information about the user. This needs to be done especially for the level column in the user table. The database should reflect the most up to date level of the user and thus if the user changes their level, the insert statement makes sure to pick the most recent level value.
4. Time: The time table does not have a primary key but contains every unique datetime timestamp in the songplay table and the break down for the timestamp for every possible date unit. 
5. Songplays: This fact table has songplay_id as a serial primary key since every record is a new fact and every time we insert a new fact record the serial primary key is incremented. This is done using the identity feature in Redshift. The information for start_time, user_id, session_id, login location and user agent come from the staging events table where as song_id and  artist_id information comes from the staging_songs table. 


## ETL pipeline
The etl pipeline is done in two steps. The first is to load data from S3 into the staging tables and then the fact and dimension tables are populated using the staging tables. The two steps are described in further details below:
    1. load_staging_tables: This function runs the load queries for the staging_events and staging_songs table. The COPY command is used to load data from the S3 data source into staging tables.
    2. insert_tables: This functions runs the load queries for the songplay, song, artist, users and time table. The ISNERT statement is used to load data into these tables from the staging tables. The data for song and artist table comes from the staging_songs table. The data for users comes from the staging_events table. Songplay table is loaded by joining both the staging tables on song and artist information. The time table is populated with the unique timestamps found in the songplay table.

## Example querries for song play analysis

1. 5 most popular artists on the streaming app
select name
    from artist a
    inner join (
        select  artist_id,
                count(\*) as number_of_plays
            from songplay
            group by artist_id
            order by count(\*) desc
            limit 5
     ) s
         on s.artist_id = a.artist_id

2. The query above can be easily adapted to find 5 most popular artists by country
       
3. 5 most popular songs on the streaming app:
select title
    from song s
    inner join (
        select  song_id,
                count(\*) as number_of_plays
            from songplay
            group by song_id
            order by count(\*) desc
            limit 5
     ) p
         on s.song_id = p.song_id

4. The query above can be easily adapted to find 5 most popular songs by country

5. Number of paid users in the current month:
declare @current_month int = 1 (January as a test case)
declare @current_year int = 2019 (2019 as a test case)
select count(distinct user_id) as number_of_paid_users
    from songplay
    where level = 'paid'
        and month(start_time) = @current_month
        and year(start_time) = @current_year