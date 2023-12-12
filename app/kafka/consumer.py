from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType
from pyspark.sql.functions import from_json, concat_ws, expr, col, to_date
from elasticsearch import Elasticsearch
import logging
from datetime import datetime

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

TOPIC_NAME = 'movies'
SERVER_NAME = 'localhost:9092'
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
    .appName("KafkaElasticsearchKibana") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1") \
    .getOrCreate()


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movies") \
    .load()

# Define the schema
schema = StructType([
    StructField('userId', StringType(), True),
    StructField('movie', StructType([
        StructField('movieId', StringType(), True),
        StructField('title', StringType(), True),
        StructField('genres', ArrayType(StringType()), True)
    ]), True),
    StructField('rating', StringType(), True),
    StructField('timestamp', StringType(), True)
])

# Parse the 'value' column
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Perform further operations or store the data
# parsed_df.writeStream.format("console").start().awaitTermination()

# Function to save DataFrame to Elasticsearch
def save_to_elasticsearch(df, index_name=ELASTICSEARCH_INDEX):
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "movies") \
        .option("es.nodes.wan.only", "true") \
        .option("es.index.auto.create", "true") \
        .mode("append") \
        .save()

# Use the function to save to Elasticsearch using WriteStream
parsed_df \
    .writeStream \
    .foreachBatch(save_to_elasticsearch) \
    .start() \
    .awaitTermination()

