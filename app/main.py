from pyspark.sql import SparkSession
from extract.interactions import getInteractions
from extract.movies import getMovies
from extract.users import getUsers
from loadTransform.interactions import loadTransactions, loadJsonInteractions
from loadTransform.movies import loadMovies
from loadTransform.users import loadUsers
import logging
from datetime import datetime
import threading

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

# Create a spark session
spark = SparkSession.builder \
    .appName("interactionsLoadTransform") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def load_and_log(func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except Exception as e:
        logging.error(f"Error in thread {threading.current_thread().name}: {str(e)}")

# Retrieve interactions data
data_interactions = getInteractions()
interactions_df_70, interactions_df_30 = data_interactions[0], data_interactions[1]

# Retrieve movies and users
movies_df, users_df = getMovies(), getUsers()

# Create threads for parallel execution
thread1 = threading.Thread(target=load_and_log, args=(loadTransactions, interactions_df_70))
thread2 = threading.Thread(target=load_and_log, args=(loadJsonInteractions, interactions_df_30))
thread3 = threading.Thread(target=load_and_log, args=(loadMovies, movies_df))
thread4 = threading.Thread(target=load_and_log, args=(loadUsers, users_df))

# Start the threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()

# Wait for all threads to finish
thread1.join()
thread2.join()
thread3.join()
thread4.join()

print("All tasks completed successfully.")
