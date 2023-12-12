from pyspark.sql import SparkSession
import sys
sys.path.append('..')
from app.extract.movies import getMovies
from pyspark.sql.functions import when
from elasticsearch import Elasticsearch
import logging
from datetime import datetime
from pyspark.sql.functions import to_date, unix_timestamp, date_format, col

# Set up a file for logs
now = str(datetime.now().year)+"-"+str(datetime.now().month)+"-"+str(datetime.now().day)

# Set up a file for logs
log_file_path = f"../spark_logs/{now}.log"

# Configure logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def loadMovies(movies_df):
        
    ELASTICSEARCH_INDEX = 'movies'

    try:
        print('Connecting to Elasticsearch')
        es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
        
        if not es.indices.exists(index='movies'):
            # Create the index
            es.indices.create(index='movies', ignore=400) 
    except Exception as e:
        logging.error(f"Can't connect to elasticsearch {str(e)}")

    # Create a spark session
    spark = SparkSession.builder \
        .appName("moviesLoadTransform") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    try:
        # Convert release date into date
        movies_df = movies_df.withColumn("release_date", date_format(col("release_date"), "yyyy-MM-dd"))

        # Function to save DataFrame to Elasticsearch
        movies_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", ELASTICSEARCH_INDEX) \
            .option("es.nodes.wan.only", "true") \
            .option("es.index.auto.create", "true") \
            .mode("append") \
            .save()

        print("Movies inserted successfully")
    except Exception as e:
        logging.error(f"Error inserting movies {str(e)}")

