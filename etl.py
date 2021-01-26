

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp
from pyspark.sql import types

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
	"""
    	Create a Spark session with a specific configuration that enables access to S3 
    	Return a Spark preconfigured instance 
    	"""
	spark = SparkSession\
			.builder\
			.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
			.getOrCreate()
	return spark

def process_song_data(spark, input_data, output_data):
	"""
    	    Read and process the raw songs json data to extract only columns forming songs_table and artists_table
 	    Load the tables back to parquet files in S3 
    	    Parameters : 

            spark     : The SparkSession instance defined above
            input_data: The S3 location prefix for raw data  
            output    : The location path for parquets files 

    	    Returns    :

            Load tables in parquet files in the "output_data" location 
   	 """
    	# get filepath to the songs data file
	song_data = input_data+'song_data/*/*/*/*.json'
	# read the songs data file
	df_songs  = spark.read.json(song_data)
	# extract columns to create songs table
	songs_table = df_songs.select('song_id','title','artist_id','year','duration').distinct()
	#Write songs table to parquet files partionned by year and artist_id
	songs_table.write.parquet(output_data+'songs.parquet', mode='overwrite', partitionBy=['year', 'artist_id'])

	artists_table = df_songs.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude').distinct()
	artists_table.write.parquet(output_data+'artists.parquet', mode='overwrite')
	# Create a Temporary view from songs_data to use it later
	df_songs.createOrReplaceTempView("songs_data")

def process_log_data(spark, input_data, output_data):
	"""
    	Read and process the raw logs json data to extract only columns forming songs_table and artists_table
    	Load the tables back to parquet files in S3 
    	Parameters : 

            spark     : The SparkSession instance defined above
            input_data: The S3 location prefix for raw data  
            output    : The location path for parquets files 

    	Returns    :

            Load tables in parquet files in the "output_data" location 
    	"""
    	# Get filepath to log data file
	log_data = input_data+"log_data/*/*/*.json"

	df_logs = spark.read.json(log_data)
	# Filter by actions for song plays
	df_logs = df_logs.where(col('page')=='NextSong')
	users_table=df_logs.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
	users_table.write.parquet(output_data+'users.parquet', mode='overwrite')
	# Create timestamp column from original timestamp column
	# This will give you a string date 
	df_logs=df_logs.withColumn('epoch',date_format(((df_logs.ts)/1000).cast(dataType=types.TimestampType()),"yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
	# Cast the string date to timestamp for further operation on the date 
	df_logs=df_logs.withColumn('start_date_time',col('epoch').cast('timestamp'))
	# Extract columns to create time table
	# Time table
	time_table=df_logs.select('start_date_time').dropDuplicates().orderBy('start_date_time', ascending=True)\
	.withColumn('hour'        , hour('start_date_time'))\
	.withColumn('day_of_month', dayofmonth('start_date_time'))\
	.withColumn('week'        , weekofyear('start_date_time'))\
	.withColumn('month'       , month('start_date_time'))\
	.withColumn('year'        , year('start_date_time'))\
	.withColumn('day'         , date_format('start_date_time', 'EEEE'))
	# Write time table to parquet files partitioned by year and month
	time_table.write.parquet(output_data+'time.parquet', mode='overwrite', partitionBy=["year", "month"])
	# Create a Temporary view from log data and time table.
	time_table.createOrReplaceTempView("time_table")
	df_logs.createOrReplaceTempView("logs_data")
	# Extract columns from joined songs and logs datasets and time table to create songplays table 
	songplays_table=spark.sql("""
                                    SELECT DISTINCT row_number() over (order by "start_time") as songplay_id,
                                                    ld.start_date_time AS start_time,
                                                    ld.userId       AS user_id,
                                                    ld.level        AS level,  
                                                    sd.song_id      AS song_id,
                                                    sd.artist_id    AS artist_id,
                                                    ld.sessionId    AS session_id,
                                                    ld.location     AS location,
                                                    ld.userAgent    AS user_agent,
                                                    tt.year,
                                                    tt.month

                                    FROM logs_data AS ld JOIN songs_data AS sd ON (ld.song=sd.title AND sd.artist_name=ld.artist)   
                                    JOIN time_table AS tt ON  tt.start_date_time = ld.start_date_time
                                    """)
        # Write songplays table to parquet files partitioned by year and month
        songplays_table.write.parquet(output_data+'songplays.parquet', mode="overwrite",partitionBy=["year","month"])

def main():
	""" 
    	Instanciate a SparkSession, load data from S3
    	Make the necessary operations and transformation and load back the formed table to parquet files in S3
    	"""
	spark=create_spark_session()
	input_data = "s3a://udacity-dend/"
	output_data = "s3://spark-demo-data-modeling/
	process_song_data(spark, input_data, output_data)
	process_log_data(spark, input_data, output_data)
if __name__ == "__main__":
	main()
