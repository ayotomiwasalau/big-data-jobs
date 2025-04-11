import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


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
    args:
        spark - initiate spark session
        input_data - Data source location (Amazon 3)
        output_data - Data output location (Amazon S3)
    
    '''
    # get filepath to song data file
    song_data_path = os.path.join(input_data,"song_data/*/*/*/*.json")
    song_data = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = song_data["song_id", "title", "artist_id", "year", "duration", "artist_name"]
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output,"songs_table.parquet"), "overwrite")

    # extract columns to create artists table
    artists_table = song_data["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output,"artists_table.parquet"), "overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    args:
        spark - initiate spark session
        input_data - Data source location (Amazon 3)
        output_data - Data output location (Amazon S3)
    
    '''
    # get filepath to song data file
    log_data_path = os.path.join(input_data,"log_data/*/*/*.json")
    

    # read log data file
    dfLog = spark.read.json(log_data_path) 
    
    # filter by actions for song plays
    df_plays = dfLog.where(dfLog.page == "NextSong")

    # extract columns for users table
    users_table = df_plays["userid", "firstName", "lastName", "gender", "level"]
    users_table = users_table.orderBy("ts",ascending=False).dropDuplicates(subset=["userId"]).drop('ts')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output,"users.parquet"), "overwrite")
    
#---------------------------------------------    
#     # create timestamp column from original timestamp column
#     get_timestamp = udf(lambda x: str(int(int(x)/1000)))
#     df = df.withColumn('timestamp', get_timestamp(df.ts))
    
#     # create datetime column from original timestamp column
#     get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
#     df = df.withColumn("datetime", get_datetime(df.ts))
    
#     # extract columns to create time table
#     time_table = df.select(
#         col('datetime').alias('start_time'),
#         hour('datetime').alias('hour'),
#         dayofmonth('datetime').alias('day'),
#         weekofyear('datetime').alias('week'),
#         month('datetime').alias('month'),
#         year('datetime').alias('year') 
#    )
#-------------------------------------------
      

    # create timestamp & datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    get_weekday = udf(lambda x: x.weekday)
    get_week = udf(lambda x: datetime.isocalendar(x)[1])
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_year = udf(lambda x: x.year)
    get_month = udf(lambda x: x.month)

    df_plays = df_plays.withColumn("start_time", get_datetime(df_plays.ts))
    df_plays = df_plays.withColumn("week_day", get_hour(df_plays.start_time))
    df_plays = df_plays.withColumn("week", get_week(df_plays.start_time))
    df_plays = df_plays.withColumn("hour", get_hour(df_plays.start_time))
    df_plays = df_plays.withColumn("day", get_day(df_plays.start_time))
    df_plays = df_plays.withColumn("year", get_year(df_plays.start_time))
    df_plays = df_plays.withColumn("month", get_month(df_plays.start_time))
    

    
    # extract columns to create time table
    time_table = df_plays.select("start_time","hour", "day", "week", "month", "year", "week_day")
    time_table = time_table.dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output,"time_table.parquet"), "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data,"song_data/*/*/*/*.json")) 

    # extract columns from joined song and log datasets to create songplays table 
    df_total = song_df.join(df_plays, (song_df.title == df_plays.song))
    songplay = df_total["start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]

    # write songplays table to parquet files partitioned by year and month
    songplay.write.parquet(os.path.join(output,"song_plays.parquet"), "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://song-event-saprk-bucket"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
