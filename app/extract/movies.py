from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, TimestampType
from pyspark.sql.functions import unix_timestamp

def getMovies():
    # Create a Spark session
    spark = SparkSession.builder.appName("getMovies").getOrCreate()

    # Read u.genre file
    genre_path = "../data/ml-100k/u.genre"
    genre_df = spark.read.csv(genre_path, sep='|', inferSchema=True, header=False).toDF('genre', 'genre_id')

    # Define column names for u.item file
    item_columns = ['movie_id', 'title', 'release_date', 'deletable', 'url'] + list(genre_df.select('genre').toPandas()['genre'])

    # Define StructType for the schema
    schema = StructType([
        StructField("movie_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("deletable", StringType(), True),
        StructField("url", StringType(), True)
    ] + [StructField(name, StringType(), True) for name in item_columns[5:]])

    # Read u.item file with explicit column names and schema
    item_path = "../data/ml-100k/u.item"
    item_df = spark.read.csv(item_path, sep='|', encoding='ISO-8859-1', inferSchema=True, header=False, schema=schema)

    # Convert 'movie_id' to integer
    item_df = item_df.withColumn("movie_id", item_df["movie_id"].cast(IntegerType()))

    # Convert 'release_date' to timestamp
    item_df = item_df.withColumn("release_date", unix_timestamp(item_df["release_date"], "dd-MMM-yyyy").cast(TimestampType()))

    # Create a UDF to convert columns to a list of genres
    def genres_to_list(*genres):
        return [genre for g, genre in zip(genres, item_columns[5:]) if g == '1']

    genres_to_list_udf = udf(genres_to_list, ArrayType(StringType()))

    # Apply the UDF to create the 'genre' column
    item_df = item_df.withColumn('genre', genres_to_list_udf(*item_df.columns[5:]))

    # Drop unnecessary columns
    columns_to_keep = ["movie_id", "title", "release_date", "url", "genre"]
    item_df = item_df.select(columns_to_keep)

    # return the dataframe
    return item_df