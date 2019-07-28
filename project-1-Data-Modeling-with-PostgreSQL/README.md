# Data Modelling with Postgress

## Script description
The Data Modelling with Postgres project uses three scripts to create the star schema and load the song and log data into the schema. The three scripts are described below:
1. sql_queries.py: This script contains all the sql querries used by the other two scripts. It is broken down into drop tables, create tables, insert records and find songs sections.
2. create_tables.py: This script first creates the sparkifydb database then drops tables using the drop tabels commands specified in sql_queries and finally creates tables usign the commands specified in sql_queries.
3. etl.py: This script loads data into the tables created from the song and log data files. process_song_file function loops through every song data file and populates the songs and artists table. process_log_file function loops through each log file and inserts records into time, users and songplays tables.
The create_tables.py must be run first to drop any existant tables and then create the tables. The etl.py must be run next to process the data from song and log files and then insert the data into the tables in the sparkifydb database.
4. test.ipynb: iPython notebook to test the data loaded into the database. The notebook contains simple queries to check the data in all five tables.

## Database context for Sparkify
This database will be critical for analytics for the start up, Sparkify. The songs and artists table track all the data in the song library. The time and users tabels track when the individual user has looged into a session. The combination of the data in these tables would provide user's listening or song playing information. The songplays fact table used to query out user's listening activity. This data can play a critical role in shaping the business decisions at the start up.

## Schema design
1. Songs: The song table has song_id as its varchar primary key and supporting columns such as song title, year of release and duration of the song. The tabel also has a artist_id column that links every song to the artist table. The two tables can be joined to query information such as what are all the songs released by one artist and so on.
2. Artists: The artist table has artist_id as its varchar primary key and supporting columns such as artist name, location, lattitude and longitude information. The location, longitude and latitude columsn are nullable as some artists locations are unknown. The artist information comes from song data files, and thus the insert statement will have to deal with duplicates since a single artists does releas multiple songs. This conflict is resolved using a do nothing statement. The logic being that the artist name information never should change and location information should not have changed that often.
3. Users: The users table has user_id as its integer primary key and supporting varchar columns such as first name, last name, gender and level. The user_id can be used as the primary_key to avoid duplicate users in the table and making user_id the primary key makes user makes sure that there will be a conflict raised during the insert statement for duplciates. This conflict is resolved, using the update statement. The level of the user is updated on the result of the conflict, logic being that the user's personal information such as name and gender shuldn't change but the user's subscription level might change over time and we would want to have the most up to date level information.
4. Time: The time table does not have a primary key but contains every datetime timestamp in the log data and the supporting columns contain the breakdown of the timestamp information. The start_time column uses a timestamp without time zone as we do not know what the timestamp's time zone is in the log data.
4. Songplays: This fact table has songplay_id as a serial primary key since every record is a new fact and every time we insert a new fact record the serial primary key is incremented. The other columns are start_time from time, user_id and level from users, song_id from songs, artist_id from artists and session_id, login location and user agent.

A suggestion or thought I had about this schema design is that we could create a session table that tracks session information for every user. So the columsn would be session_id primary_key, user_id a link to users table, location, user_agent. The advantage of doing this in my opinion would be that we could query out all the columns needed for the songplay fact table form the dimension table instead of loading data from the log data file and writing the select song qeury every time we load a record.

## ETL pipeline
1. process_data function is called twice. Once to process song data in function process_song_file and another time to process log data in fucntion process_log_file. process_data function loops over every song or log data file available and processes them individually.
    1. process_song_file: Receives an individual song data file and extracts song and artist data from the file and loads records in the song and artist table. This function is called for every song data file available. The song data is extracted from a panda dataframe created from the song data json file.
    2. process_log_file: Receives an individual log data file and extracts user and time data from the file and loads records in the time and user table. The user and timestamp data is extracted from a panda dataframe created from the log data json file. This function is also in charge of populating the songplay fact table. When we find a log entry for a song play that matches one of the songs in the song table we load a record in the songplay table.

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
