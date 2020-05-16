import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, from_unixtime
from pyspark.sql.types import IntegerType, TimestampType
from datetime import datetime
from pyspark.sql import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('header','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('header','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Summary:
    Creates and return spark session
    
    Returns:
    spark: Spark session created.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Summary:
    Read song data and process it and save to provided output location
    
    Parameters:
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    print(song_data)
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").distinct()
    songs_table.show(20,False)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data+"songs")

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"),
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude")).distinct()
        
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists")

def process_log_data(spark, input_data, output_data):
    """
    Summary:
    Read log data and process it and save to provided output location
    
    Parameters:
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")
    
    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            col("gender").alias("gender"),
                            col("level")).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x)
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select(col("start_time"),col("datetime"),
                           hour("datetime").alias("hour"),
                           dayofmonth("datetime").alias("day"),
                           weekofyear("datetime").alias("week"),
                           month("datetime").alias("month"),
                           year("datetime").alias("year"),
                           dayofweek("datetime").alias("weekday")) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data+"time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+"song_data/*/*/*/*.json").select("song_id","title","artist_id","artist_name").distinct()

    window = Window.orderBy(col('ts'))
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.artist==song_df.artist_name) & (df.song==song_df.title)).withColumn('songplay_id',row_number().over(window)).select("songplay_id",
                                "start_time",
                                "userId",
                                "level",
                                "song_id",
                                "artist_id",
                                "sessionId",
                                "location",
                                "userAgent",
                                month("datetime").alias("month"),
                                year("datetime").alias("year"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")

def main():
    """
    Summary:
    Entry point of the etl
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-engineer-nanodegree-mm/data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
