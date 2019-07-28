import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE if not exists staging_events 
(
  eventId     	BIGINT identity(0,1),
  artist        VARCHAR(256) NULL,
  auth        	VARCHAR(256) NULL,
  firstName    	VARCHAR(256) NULL,
  gender      	VARCHAR(1) NULL,
  itemInSession	BIGINT NOT NULL,
  lastName      VARCHAR(256) NULL,
  length        DOUBLE PRECISION NULL,
  level   		VARCHAR(256) NULL,
  location   	VARCHAR(256) NULL,
  method   		VARCHAR(256) NULL,
  page   		VARCHAR(256) NULL,
  registration  BIGINT NULL,
  sessionId   	BIGINT NULL,
  song   		VARCHAR(256) NULL,
  status   		BIGINT NULL,
  ts   			BIGINT NULL,
  userAgent   	VARCHAR(256) NULL,
  userId   		BIGINT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE if not exists staging_songs 
(
  artist_id			VARCHAR(256) NULL,
  artist_latitude 	DOUBLE PRECISION NULL,
  artist_longitude 	DOUBLE PRECISION NULL,
  artist_location	VARCHAR(256) NULL,
  artist_name		VARCHAR(256) NULL,
  song_id			VARCHAR(256) NULL,
  title				VARCHAR(256) NULL,
  duration			DOUBLE PRECISION NULL,
  songYear			BIGINT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE if not exists songplay (
  songplay_id 	int identity(0,1) PRIMARY KEY,
  start_time 	timestamp not null,
  user_id 		int not null,
  level     	varchar(256) not null,
  song_id 		varchar(256) not null,
  artist_id 	varchar(256) not null,
  session_id 	int not null,
  location     	varchar(256) not null,
  user_agent    varchar(256) not null
);
""")

user_table_create = ("""
CREATE TABLE if not exists users (
  user_id    	integer not null PRIMARY KEY,
  first_name	varchar(256) not null,
  last_name     varchar(256) not null,
  gender     	varchar(1) not null,
  level     	varchar(256) not null
);
""")

song_table_create = ("""
CREATE TABLE if not exists song (
  song_id	varchar(256) not null PRIMARY KEY,
  title		varchar(256) not null,
  artist_id	varchar(256) not null,
  year    	integer null,
  duration  double precision not null
);
""")

artist_table_create = ("""
CREATE TABLE if not exists artist (
  artist_id	varchar(256) not null PRIMARY KEY,
  name		varchar(256) not null,
  location	varchar(256) null,
  latitude  double precision null,
  longitude	double precision null
);
""")

time_table_create = ("""
CREATE TABLE if not exists time (
  start_time 	timestamp not null PRIMARY KEY,
  hour			integer not null,
  day			integer not null,
  week    		integer not null,
  month  		integer not null,
  t_year  		integer not null,
  weekday  		integer not null
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off
    JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off
    JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay
(
    start_time,
  	user_id,
  	level,
  	song_id,
  	artist_id,
  	session_id,
  	location,
  	user_agent
)
select  distinct 
        TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second',
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
   		se.sessionId,
        se.location,
        se.useragent
   	from staging_events se
    inner join staging_songs ss
    	on 	ss.artist_name = se.artist
        and ss.title = se.song
        and ss.duration = se.length
    where se.page = 'NextSong' ;
""")

user_table_insert = ("""
insert into users
(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
select 	s1.userId,
		s1.firstname,
        s1.lastname,
        s1.gender,
        s1.level
	from staging_events s1
	inner join (
      select  userId,
              MAX(TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as most_recent_time
          from staging_events
          group by userId
	) s2
    	on  s2.userId = s1.userId
        and s2.most_recent_time = TIMESTAMP 'epoch' + s1.ts/1000 * interval '1 second' ;
""")

song_table_insert = ("""
insert into song
(
    song_id,
    title,
    artist_id,
    year,
    duration
)
select  distinct 
        song_id,
        title,
        artist_id,
        songYear,
        duration
    from staging_songs ;
""")

artist_table_insert = ("""
insert into artist
(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
select  distinct 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs ;
""")

time_table_insert = ("""
insert into time
(
    start_time,
    hour,
    day,
    week,
    month,
    t_year,
    weekday
)
select 	distinct 
		start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
	from songplay ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
