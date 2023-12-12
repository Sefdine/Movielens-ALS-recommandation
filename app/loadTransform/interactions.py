from pyspark.sql import SparkSession
import sys
sys.path.append('..')
from app.extract.interactions import getInteractions
from elasticsearch import Elasticsearch
import logging
from datetime import datetime
from pyspark.sql.functions import to_date, unix_timestamp, from_unixtime, when, col, date_format

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

ELASTICSEARCH_INDEX = 'interactions'

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
    .appName("interactionsLoadTransform") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

interactions_df = getInteractions()

try:
    # Convert the integer timestamp to a timestamp type
    interactions_df = interactions_df.withColumn("timestamp", from_unixtime(col("timestamp")))

    # Apply date_format function to convert timestamp to date with the desired format
    interactions_df = interactions_df.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd"))

    # Split the DataFrame into 70% and 30%
    split_df = interactions_df.randomSplit([0.7, 0.3], seed=42)
    
    # Extract 70% DataFrame
    interactions_70_df = split_df[0]

    # Extract 30% DataFrame
    interactions_30_df = split_df[1]

    # Function to save 70% DataFrame to Elasticsearch
    interactions_70_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", ELASTICSEARCH_INDEX) \
        .option("es.nodes.wan.only", "true") \
        .option("es.index.auto.create", "true") \
        .mode("append") \
        .save()

    print("Interactions (70%) inserted successfully into Elasticsearch")

    # Convert PySpark DataFrame to Pandas DataFrame
    interactions_30_pandas = interactions_30_df.toPandas()

    # Save 30% DataFrame to a single JSON file using Pandas
    interactions_30_pandas.to_json("../api/interactions_30.json", orient="records", lines=True)

    print("Interactions (30%) saved successfully to JSON file")
except Exception as e:
    logging.error(f"Error inserting interactions {str(e)}")