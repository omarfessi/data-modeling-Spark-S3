

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
	spark = SparkSession\
			.builder\
			.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
			.getOrCreate()
	return spark

def process_song_data(spark, input_data, output_data):
	song_data = input_data+'song_data/*/*/*/*.json'
	df_songs  = spark.read.json(song_data)
	songs_table = df_songs.select('song_id','title','artist_id','year','duration').distinct()
	songs_table.write.parquet(output_data+'songs.parquet', mode='overwrite', partitionBy=['year', 'artist_id'])
	artists_table = df_songs.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude').distinct()
	artists_table.write.parquet(output_data+'artists.parquet', mode='overwrite')
	# Create a Temporary view from songs_data to use it later
	df_songs.createOrReplaceTempView("songs_data")

def process_log_data(spark, input_data, output_data):
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
	spark=create_spark_session()
	input_data = "s3a://udacity-dend/"
	output_data = "s3://spark-demo-data-modeling/
	process_song_data(spark, input_data, output_data)
	process_log_data(spark, input_data, output_data)
if __name__ == "__main__":
	main()
