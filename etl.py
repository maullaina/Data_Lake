import configparser
from datetime import datetime
import os
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import  pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Spark session is a unified entry point of a spark application from 
        Spark 2.0. It provides a way to interact with various spark's functionality 
        with a lesser number of constructs

        return: None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def get_client():
    """
        You use the AWS SDK for Python (Boto3) to create, configure, and manage 
        AWS services, such as s3.

        return {obj}: s3 -- Client is a low level class object.
    """
    s3 = boto3.client(
    's3',
    aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    region_name= 'us-east-1')
    return s3

def process_song_data(spark, input_data, output_data):
    """ Get information about songs and artists from raw data and introduce it
        in the a s3 AWS bucket in parquet format.

        Arguments:
            spark {object}: entry point of a spark application
            input_data {str}: a string with the file path for the s3 bucket where the 
            raw data is allocated.
            output_data {str}: a string with the file path for the s3 bucket where the 
            parquet files will be stored.
            
        Returns:
            None
    """
    # get filepath to song data file
    s3=get_client() 
    response = s3.list_objects_v2(
        Bucket="udacity-dend",
        Prefix ='song_data/')

    content=response.get('Contents')
    for r in content[1:]:
        print(r.get('Key'))
        path= input_data + r.get('Key')
        # read song data file
        df = spark.read.json(path).drop_duplicates()        

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    df_songs = spark.sql("""SELECT DISTINCT song_id,
                title,
                artist_id, 
                year,
                duration       
                FROM songs""")
    
    # write songs table to parquet files partitioned by year and artist
    pqt_folder= output_data + 'tables/'
    pqt_songs= pqt_folder + 'songs.parquet'
    df_songs.write.partitionBy("year","artist_id").mode("overwrite").parquet(pqt_songs)

    # extract columns to create artists table
    df.createOrReplaceTempView("artists")
    df_artist= spark.sql("""SELECT DISTINCT artist_id,
                artist_name,
                title,
                artist_location,
                artist_latitude,
                artist_longitude      
                FROM artists""")
    
    # write artists table to parquet files
    pqt_artists= pqt_folder + 'artists.parquet'
    df_artist.write.mode("overwrite").parquet(pqt_artists)


def process_log_data(spark, input_data, output_data):
    """ Get information about time and users from raw data and introduce it
        in the tables in parquet format and create the fact table songplays.

        Arguments:
            spark {object}: entry point of a spark application
            input_data {str}: a string with the file path for the s3 bucket where the 
            raw data is allocated.
            output_data {str}: a string with the file path for the s3 bucket where the 
            parquet files will be stored.
            
        Returns:
            None
    """
    # get filepath to log data file
    s3=get_client() 
    response = s3.list_objects_v2(
        Bucket="udacity-dend",
        Prefix ='log_data/')

    content=response.get('Contents')
    for r in content[1:]:
        print(r.get('Key'))
        path="s3a://udacity-dend/" + r.get('Key')
        # read log data file
        df = spark.read.json(path).drop_duplicates()        
        
    # filter by actions for song plays
    df_nxt = df.page=='NextSong'
    df = df[df_nxt]

    # extract columns for users table    
    df.createOrReplaceTempView("users")
    df_user= spark.sql("""SELECT DISTINCT userId,
                firstName, 
                lastName, 
                gender, 
                level      
                FROM users""")
    
    # write users table to parquet files
    pqt_folder= output_data + 'tables/'
    pqt_users= pqt_folder + 'users.parquet'
    df_user.write.mode("overwrite").parquet(pqt_users)

    # create timestamp column from original timestamp column
    df = df.withColumn("ts",df.ts/1000)
    df = df.withColumn("datetime", F.to_timestamp("ts"))
    
    # extract columns to create time table
    df = df.withColumn("hour", F.hour("datetime"))
    df = df.withColumn("day", date_format(col("datetime"), "d"))
    df = df.withColumn("week", date_format(col("datetime"), "w"))
    df = df.withColumn("month", F.month("datetime"))
    df = df.withColumn("year", date_format(col("datetime"), "y"))
    df = df.withColumn("weekday", date_format(col("datetime"), "u"))
    
    df.createOrReplaceTempView("time")
    df_time= spark.sql("""SELECT DISTINCT datetime as start_time, 
                hour, 
                day,
                week,
                month, 
                year,
                weekday    
                FROM time""")
    
    # write time table to parquet files partitioned by year and month
    pqt_time= pqt_folder + 'time.parquet'
    df_time.write.partitionBy("year","month").mode("overwrite").parquet(pqt_time)
    
    # read in song data to use for songplays table
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    song_table = output_data + 'tables/songs.parquet'
    df_songs=spark.read.parquet(song_table)
    df_songs.createOrReplaceTempView("songs")
    
    # extract columns from joined song and log datasets to create songplays table 
    df.createOrReplaceTempView("songplays")
    df_songplays= spark.sql("""SELECT songplays.songplay_id, 
            songplays.song,
            songplays.datetime as start_time, 
            songplays.userId, 
            songplays.level, 
            songplays.sessionId, 
            songplays.location, 
            songplays.userAgent, 
            songplays.year, 
            songplays.month, 
            
            songs.song_id, 
            songs.artist_id, 
            songs.title
            FROM songplays
            JOIN songs ON songplays.song = songs.title""")

    # write songplays table to parquet files partitioned by year and month
    pqt_songplays= pqt_folder + 'songplays.parquet'
    df_songplays.write.partitionBy("year","month").mode("overwrite").parquet(pqt_songplays)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
