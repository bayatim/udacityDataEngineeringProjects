import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql.dataframe as psd
import pyspark.sql.session as pss

# load config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# add credentials to environment variable
os.environ['AWS_ACCESS_KEY_ID']=  config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']= config['AWS']['AWS_SECRET_ACCESS_KEY']


class Reader:
    """[Reader class loads log data for further usage. It is written with Singleton pattern to prevent loading data
    multiple time, as the process is very slow.]

    Returns:
        [type]: [the instance of class]
    """
    _instance = None
    
    def __init__(self, *args, **kwargs):
        pass
    
    def __new__(cls, *args, **kwargs):
        """[initializes a new instance of class if not existed. It initializes parameters, creates a spark session, loads data]

        Returns:
            [type]: [inputpath, output path, spark session, and data]
        """
        if not cls._instance:
            # create inatance
            spark_reader = super(Reader, cls).__new__(cls, *args, **kwargs)
            # fit parameters and data
            spark_reader.fit()
            cls._instance = spark_reader
        
        return cls._instance
        
    def parameters(self) -> str:
        """[initializes necessary parameters]

        Returns:
            [str]: [path to read and write data]
        """
        input_data_path = "s3a://udacity-dend"
        output_data_path = "s3a://datalakebucketudacity"
        return input_data_path, output_data_path
        
    def create_spark_session(self) -> pss.SparkSession: 
        """[initialize a spark session]

        Returns:
            [pyspark.sql.session.SparkSession]: [created spark session]
        """
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
            .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
            .getOrCreate()

        return spark
        
    def load_song_data(self) -> psd.DataFrame:
        """[loads song data with the predefined schema]

        Returns:
            [pyspark.sql.dataframe.DataFrame]: [song data as spark dataframe]
        """
        
        # define schema
        song_data_schema = T.StructType([
            T.StructField("song_id", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("artist_id", T.StringType()),
            T.StructField("year", T.IntegerType()),
            T.StructField("duration", T.FloatType()),
            T.StructField("artist_name", T.StringType()),
            T.StructField("artist_location", T.StringType()),
            T.StructField("artist_latitude", T.DecimalType(8, 6)),
            T.StructField("artist_longitude", T.DecimalType(9, 6)),
        ])

        # get filepath to song data file
        song_data_path = os.path.join(self.input_data_path, "song_data/*/*/*/*.json") #"song_data/A/B/C/TRABCEI128F424C983.json")
        
        #read song data file
        song_df = self.spark.read.json(song_data_path, schema=song_data_schema)

        return song_df


    def load_log_data(self) -> psd.DataFrame:
        """[loads log data with predefined schema]

        Returns:
            [pyspark.sql.dataframe.DataFrame]: [log data as spark dataframe]
        """

        # define schema
        log_data_schema = T.StructType([
                            T.StructField("artist", T.StringType()),
                            T.StructField("auth", T.StringType()),
                            T.StructField("firstName", T.StringType()),
                            T.StructField("gender", T.StringType()), #optimierensbedarf
                            T.StructField("itemInSession", T.IntegerType()),
                            T.StructField("lastName", T.StringType()),
                            T.StructField("length", T.DoubleType()), 
                            T.StructField("level", T.StringType()),
                            T.StructField("location", T.StringType()),
                            T.StructField("method", T.StringType()),
                            T.StructField("page", T.StringType()),
                            T.StructField("registration", T.StringType()),
                            T.StructField("sessionId", T.IntegerType()),
                            T.StructField("song", T.StringType()),
                            T.StructField("status", T.StringType()),
                            T.StructField("ts", T.LongType()), 
                            T.StructField("userAgent", T.StringType()),
                            T.StructField("userId", T.StringType()), #optimierensbedarf
                            ])

        # get filepath to log data file
        log_data_path = os.path.join(self.input_data_path, "log_data/*/*/*.json") #2018-11-13-events.json

        #read log data file
        log_df = self.spark.read.json(log_data_path, schema=log_data_schema) 

        return log_df
    
    def fit(self):
        """[fits necessary parameters, creats spark session, and loads log and song data.]

        Returns:
            [type]: []
        """

        # get root path to read and write data
        self.input_data_path, self.output_data_path = self.parameters()
        
        # get spark session
        self.spark = self.create_spark_session()
        
        print("Start fitting song data to reader class ...")
        self.song_df = self.load_song_data()
        print("End fitting song data to reader class!")
        
        print("Start fitting log data to reader class ...")
        self.log_df = self.load_log_data()
        print("End fitting log data to reader class!")
        
        return True

    
def process_song_data(spark: pss.SparkSession, song_df: psd.DataFrame, output_data_path: str):
    """[Extract columns from song data, creates songs and artists tables, and writes them to output path]
    """
    # extract columns to create songs table
    songs_table = song_df.selectExpr('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # create a temporal view 
    songs_table.createOrReplaceTempView("songs_table_view")
    
    # remove null values from song_id column and drop duplicates
    songs_table_clean = spark.sql("""
                                  WITH STC AS(
                                      SELECT song_id, title, artist_id, year, duration, 
                                             ROW_NUMBER() OVER(PARTITION BY song_id ORDER BY artist_id NULLS LAST) AS RowNumber
                                      FROM songs_table_view
                                      WHERE song_id IS NOT NULL
                                  )
                                  SELECT song_id, title, artist_id, year, duration
                                  FROM STC
                                  WHERE RowNumber = 1
                                  """)

    # write songs table to parquet files partitioned by year and artist
    print("Start writting songs table to output path ....")
    path_write_songs_table = os.path.join(output_data_path, "analytics/songs/songs_table.parguet")
    songs_table_clean.write.partitionBy("year", "artist_id").mode("overwrite").parquet(path_write_songs_table)
    print("End writting songs table to output path!")
    
    # extract columns to create artists table
    artists_table = song_df.selectExpr('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # create temporal view
    artists_table.createOrReplaceTempView('artists_table_view')
    
    # remove null values from artist_id column and drop duplicates
    artists_table_clean = spark.sql("""
                                  WITH ATC AS(
                                      SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
                                             ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY artist_id) AS RowNumber
                                      FROM artists_table_view
                                      WHERE artist_id IS NOT NULL
                                  )
                                  SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                  FROM ATC
                                  WHERE RowNumber = 1
                                  """)
                                       
    # write artists table to parquet files
    print("Start writting artists table to output path ....")
    path_write_artists_table = os.path.join(output_data_path, "analytics/artists/artists_table.parguet")
    artists_table_clean.write.mode("overwrite").parquet(path_write_artists_table)
    print("End writting artists table to output path!")

def process_log_data(spark: pss.SparkSession, log_df: psd.DataFrame, song_df: psd.DataFrame, output_data_path: str):
    """[Extract columns from log data, creates users, time, and songplay tables, and writes them to output path]
    """
    # filter by actions for song plays
    #df = 

    # extract columns for users table    
    users_table = log_df.selectExpr("userId", "firstName", "lastName", "gender", "level", "ts", "page")
    
    # create temporal view
    users_table.createOrReplaceTempView("users_table_view")
    
    # filter by actions for song plays and eliminate NULL values and drop duplicates of user_id column
    users_table_clean = spark.sql("""
                            SELECT  DISTINCT(se.userId) AS user_id,
                                    se.firstName             AS first_name,
                                    se.lastName              AS last_name,
                                    se.gender,
                                    se.level
                            FROM users_table_view AS se
                            RIGHT JOIN 
                            (
                                SELECT  userId,
                                        MAX(ts) AS ts
                                FROM users_table_view
                                WHERE userId is not NULL AND page = 'NextSong'
                                GROUP BY userId
                            ) AS sem
                            ON se.userId = sem.userId AND se.ts = sem.ts
                            WHERE se.userId is not NULL AND se.page = 'NextSong'
                            """)

    # write users table to parquet files
    print("Start writting users table to output path ....")
    path_write_users_table = os.path.join(output_data_path, "analytics/users/users_table.parguet")
    users_table_clean.write.mode("overwrite").parquet(path_write_users_table)
    print("End writting users table to output path!")
    
    # create timestamp column from original timestamp column
    ## extract columns from log dataframe
    time_table = log_df.selectExpr("cast(ts as bigint) AS start_time", "page")
    
    ## register function to extract time stamp from bigint
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0) if x is not None else x, T.TimestampType())
    
    ## add timestamp column
    time_table = time_table.withColumn("time_stamp", get_timestamp("start_time"))
    
    # create temporal view
    time_table.createOrReplaceTempView("time_table_view")
    
    # filter by actions for song plays, eliminate NULL values and drop duplicates of timestamp column, and extract columns to create time table
    time_table_clean = spark.sql(""" 
                                WITH tsd AS (
                                    SELECT DISTINCT(time_stamp)
                                    FROM time_table_view
                                    WHERE page = 'NextSong' AND time_stamp is not NULL
                                )
                                SELECT  tsd.time_stamp,
                                        date(tsd.time_stamp)       AS date,
                                        hour(tsd.time_stamp)       AS hour,
                                        dayofmonth(tsd.time_stamp) AS day,
                                        weekofyear(tsd.time_stamp) AS week,
                                        month(tsd.time_stamp)      AS month,
                                        year(tsd.time_stamp)       AS year,
                                        weekday(tsd.time_stamp)    AS weekday
                                from tsd
                            """)
    

    # write time table to parquet files partitioned by year and month
    print("Start writting time table to output path ....")
    path_write_time_table = os.path.join(output_data_path, "analytics/time/time_table.parguet")
    time_table_clean.write.partitionBy("year", "month").mode("overwrite").parquet(path_write_time_table)
    print("End writting time table to output path!")
    
    # create view of song data to use for songplays table
    song_df.createOrReplaceTempView("song_view")

    # add timestamp column to log dataframe 
    new_log_df = log_df.withColumn("time_stamp", get_timestamp("ts"))
    
    # create temporal view
    new_log_df.createOrReplaceTempView("log_view") 

    # extract columns from joined song and log datasets to create songplays table
    songplay_table = spark.sql(""" 
                            SELECT  lv.ts                 AS start_time,
                                    month(lv.time_stamp)  AS month,
                                    year(lv.time_stamp)   AS year,
                                    lv.userId             AS user_id,
                                    lv.level,
                                    sv.song_id,
                                    sv.artist_id,
                                    lv.sessionId          AS session_id,
                                    lv.location,
                                    lv.userAgent
                            FROM log_view AS lv
                            JOIN song_view AS sv
                            ON lv.artist = sv.artist_name
                            WHERE lv.page = 'NextSong'
                            """) 

 
    # write songplays table to parquet files partitioned by year and month
    print("Start writting songplay table to output path ....")
    path_write_songplay_table = os.path.join(output_data_path, "analytics/songplay/songplay_table.parguet")
    songplay_table.write.partitionBy("year", "month").mode("overwrite").parquet(path_write_songplay_table)
    print("End writting songplay table to output path!")
    

def main():
    """[initializes Reader class and processes song and log data]
    """ 
    reader = Reader()
    
    process_song_data(reader.spark, reader.song_df, reader.output_data_path)
    process_log_data(reader.spark, reader.log_df, reader.song_df, reader.output_data_path)
    

if __name__ == "__main__":
    main()
