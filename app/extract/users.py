from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def getUsers():
    # Create a Spark session
    spark = SparkSession.builder.appName("getUsers").getOrCreate()

    # Define the schema based on your column names and data types
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("zip", StringType(), True)
    ])

    # Specify the file path
    user_path = "../data/ml-100k/u.user"

    # Read the CSV file with the specified schema
    user_df = spark.read.csv(user_path, sep='|', header=False, schema=schema)

    # Return the dataframe
    return user_df