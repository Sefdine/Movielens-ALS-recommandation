from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

def getInteractions():
    # Create a Spark session
    spark = SparkSession.builder.appName("getInteractions").getOrCreate()

    # Define StructType for the schema
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("movie_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    # Read u.data file
    data_path = "../data/ml-100k/u.data"
    data_df = spark.read.csv(data_path, header=False, schema=schema, sep='\t')

    return data_df