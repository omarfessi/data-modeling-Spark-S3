from spark.sql import SparkSession

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