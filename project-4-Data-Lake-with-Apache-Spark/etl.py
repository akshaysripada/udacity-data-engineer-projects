import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, to_date
from pyspark.sql.types import IntegerType, TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark

def process_song_data(spark, input_data, output_data):
    '''
        Process song data into song and artist tables
        Parameters:
            - spark: Spark application object
            - input_data: Path to base input data
            - output_data: Path to base output data
            - song_data: path to song data files
            - df: song data dataframe
            - songs_table: dataframe that holds the song table records
            - artists_table: dataframe that holds the artist table records
       Outputs:
           None
    '''
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + 'song', mode='overwrite')
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', col('artist_name').alias('name'), col('artist_location'). \
                                   alias('location'),col('artist_latitude').alias('latitude'), \
                                   col('artist_longitude').alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artist', mode='overwrite')
    
def process_log_data(spark, input_data, output_data):
    '''
        Process log data into users, time and songplay tables
        Parameters:
            - spark: Spark application object
            - input_data: Path to base input data
            - output_data: Path to base output data
            - log_data: path to log data files
            - df: log data dataframe
            - users_temp: dataframe to store the most recent timestamp (login)
                            for every unique user in log data
            - users: dataframe that holds the user table records
            - time_table: dataframe that holds the time table records
            - song_data: path to song data files
            - song_df: song data dataframe
            - songplays_table: dataframe that holds the songplats table records
       Outputs:
           None
    '''
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create a temp user data frame that contains the most recent timestamp 
    # corresponding to every userId except the blank userId (usedId == '')
    users_temp = df.filter(df.userId != ''). \
                    groupBy('userId'). \
                    max('ts'). \
                    select('userId', col('max(ts)').alias('max_ts'))
    
    # extract columns for users table    
    users_table = df.join(users_temp, [df.userId == users_temp.userId, df.ts == users_temp.max_ts]). \
                    select(df.userId.alias('user_id'), df.firstName.alias('first_name'), \
                    df.lastName.alias('last_name'), df.gender, df.level)
    # cast user_id column from string to int
    users_table = users_table.withColumn('user_id', users_table['user_id'].cast(IntegerType()))
    
    # write users table to parquet files
    users_table.write.partitionBy('user_id').parquet(output_data + 'users', mode='overwrite')
    
    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', from_unixtime(df.ts/1000).cast(TimestampType()))
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', from_unixtime(df.ts/1000).cast(DateType()))
    
    # extract columns to create time table
    time_table = df.select(df.timestamp.alias('start_time'), hour('timestamp').alias('hour'), \
                       dayofmonth('timestamp').alias('day'), weekofyear('timestamp').alias('week'), \
                       month('timestamp').alias('month'), year('timestamp').alias('year'), \
                       dayofweek('timestamp').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time', mode='overwrite')
    
    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*'
    song_df = spark.read.json(song_data)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song) & \
                        (song_df.artist_name == df.artist) & \
                        (song_df.duration == df.length), 'inner'). \
                        select(monotonically_increasing_id().alias('songplay_id'), \
                        df.timestamp.alias('start_time'), df.userId.alias('user_id'), \
                        df.level, song_df.song_id, song_df.artist_id, df.sessionId.alias('session_id'), \
                        df.location, df.userAgent.alias('user_agent'), year(df.timestamp).alias('year'), \
                        month(df.timestamp).alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays', mode='overwrite')
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3a://analyticstables/analytics/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()
