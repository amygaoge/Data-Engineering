import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','SECRET')


def create_spark_session():
    """ 
    The function to create spark session
    """
    
    try:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        
        print('SparkSession Created')
        
    except:
        print('Error: Can''t create SparkSession')
        
    return spark


def process_song_data(spark, input_data_path, output_data_path):
    """ 
    The function to process song file and write song and artist tables to parquet
  
    Parameters: 
        spark (object): The spark session
        input_data_path (string): The S3 bucket path for input raw files
        output_data_path (string): The S3 bucket path for output table files
    """
    
    
    try:
        # get filepath to song data file
        song_path = '{input_data}/song_data/A/A/A/*.json'.format(input_data = input_data_path)
        print(song_path)

        # read song data file
        song_data_df = spark.read.json(song_path)
        
        print('Success: Read song_data from S3')
        
    except Exception as e:
        print('Error: Can''t Read song data from S3')
        print(e)
        
    
    try:
        # extract columns to create songs table
        song_table = song_data_df.select('song_id', 'title', 'artist_id',
                                'year', 'duration').dropDuplicates(['song_id'])

        song_table = song_table.withColumnRenamed('title','song_title')\
                .withColumnRenamed('year','song_year')\
                .withColumnRenamed('duration','song_duration')

        # write songs table to parquet files partitioned by year and artist
        # write songs table to parquet files partitioned by year and artist
        song_table.write.partitionBy('song_year','artist_id').parquet('{output_data}/song_table.parquet'.format(output_data=output_data_path),
                                  mode='overwrite')
        
        print('Success: Wrote songs_table to parquet')
    except Exception as e:
        print('Error: Cann''t process songs_table')
        print(e)
        
    
    try:
        # extract columns to create artists table
        artist_table = song_data_df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates(['artist_id'])

        # write artists table to parquet files
        artist_table.write.parquet('{output_data}/artist_table.parquet'.format(output_data=output_data_path), mode ='overwrite')
        
        print('Success: Wrote artists_table to parquet')

    except Exception as e:
        print('Error: Cann''t process artists_table')
        print(e)
        


def process_log_data(spark, input_data_path, output_data_path):
    """ 
    The function to process log file and write user, time and songplay tables to parquet
  
    Parameters: 
        spark (object): The spark session
        input_data_path (string): The S3 bucket path for input raw files
        output_data_path (string): The S3 bucket path for output table files
    """
    
    try:
        # get filepath to log data file
        log_data = '{input_data}/log_data/*/*/2018-11-01-events.json'.format(input_data=input_data_path)
        print(log_data)

        # read log data file
        log_data_df = spark.read.json(log_data)

        # filter by actions for song plays
        log_data_df = log_data_df.where(log_data_df.page == 'NextSong')
        
        print('Success: Read log_data from S3')

    except Exception as e:
        print('Error: Can''t Read log data from S3')
        print(e)
        
    
    try:
        # extract columns for users table    
        user_table = log_data_df.select('userId','firstName','lastName','gender','level').dropDuplicates(['userId'])
        user_table = user_table.withColumnRenamed('userId','user_id')\
        .withColumnRenamed('firstName','user_first_name')\
        .withColumnRenamed('lastName','user_last_name')\
        .withColumnRenamed('gender','user_gender')\
        .withColumnRenamed('level','user_level')

        # write users table to parquet files
        user_table.write.parquet('{output_data}/user_table.parquet'.format(output_data=output_data_path), mode = 'overwrite')
        
        print('Success: Wrote user_table to parquet')

    except Exception as e:
        print('Error: Cann''t process user_table')
        print(e)
                                 
    
    try:  
        # create timestamp column from original timestamp column
        get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
        timestamp = log_data_df.withColumn("start_time", get_timestamp('ts'))

        # extract columns to create time table
        time_table = timestamp.select('ts', 'start_time')\
                            .withColumn('time_hour', F.hour('start_time'))\
                            .withColumn('time_week', F.weekofyear('start_time'))\
                            .withColumn('time_day', F.dayofmonth('start_time'))\
                            .withColumn('time_month', F.month('start_time'))\
                            .withColumn('time_year', F.year('start_time'))\
                            .withColumn('time_weekday', F.dayofweek('start_time')).dropDuplicates(['start_time'])

        # write time table to parquet files partitioned by year and month
        time_table.write.parquet('{output_data}/time_table.parquet'.format(output_data=output_data_path),mode='overwrite', partitionBy = ['time_year','time_month'])
        
        print('Success: Wrote time_table to parquet')
    except Exception as e:
        print('Error: Cann''t process time_table')
        print(e)
                                 
                                 
    try:
        # read in song data to use for songplays table
        song_path = '{input_data}/song_data/A/A/A/*.json'.format(input_data = input_data_path)
        song_data_df = spark.read.json(song_path)
        print('Success: Read song_dataset from S3')
        
        log_data_df.createOrReplaceTempView('log_data')
        song_data_df.createOrReplaceTempView('song_data')
        time_table.createOrReplaceTempView('time_table')
        
        # extract columns from joined song and log datasets to create songplays table 
        songplay_table = spark.sql('''
                    SELECT monotonically_increasing_id() as songplay_id, t.start_time, l.userId as user_id, l.level, 
                    s.song_id, s.artist_id, 
                    t.time_year, t.time_month,
                    l.sessionId as session_id, l.location, l.userAgent as user_agent
                    FROM log_data l
                    LEFT JOIN song_data s on l.song = s.title and l.artist = s.artist_name and l.length = s.duration
                    LEFT JOIN time_table t on l.ts = t.ts
                    WHERE l.page = "NextSong" ''')

        # write songplays table to parquet files partitioned by year and month
        songplay_table.write.partitionBy('time_year','time_month').parquet('{output_data}/songplay_table.parquet'.format(output_data=output_data_path), mode = 'overwrite')
        
        print('Success: Wrote songplays_table to parquet')
        
    except Exception as e:
        print('Error: Cann''t process songplay_table')
        print(e)


def main():
    print('ETL started.')
    
    spark = create_spark_session()
    input_data_path = "s3a://udacity-dend/"
    output_data_path = "s3a://udacity-spark"
    
    process_song_data(spark, input_data_path, output_data_path)    
    print('Finish processing song data')
    
    process_log_data(spark, input_data_path, output_data_path)
    print('Finish processiong log data')
                             
    print('ETL done.')


if __name__ == "__main__":
    main()
