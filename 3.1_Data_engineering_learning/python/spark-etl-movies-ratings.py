import os
import sys

from pyspark.sql import SparkSession
from google.cloud import bigquery

# Required if at some point you got 
# 'java.io.IOException: Cannot run program "python3": CreateProcess error=2, The system cannot find the file specified'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Build the spark session
spark = SparkSession.builder.appName("Movies ETL - ratings").getOrCreate()

# Load data file from cloud storage
ratings_file = "gs://course_data_engineering_sample_data/movie-datasets/IMDb_ratings.gz"
ratings_spark = spark.read.csv(ratings_file, header=True)

# TODO : replace google cloud project id with your own
project_id = "timpamungkas-udemy" 
# TODO : replace the 'temporaryGcsBucket' with your own google cloud storage bucket
temporaryGcsBucket = "course_data_engineering_spark_temp"

# create bigquery dataset
client = bigquery.Client()
dataset_name = "spark_dataset"
dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
dataset.location = "us-east1"
dataset.default_table_expiration_ms = 3600000
dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)

# Write to bigquery
ratings_spark.write.format("bigquery")\
    .option("temporaryGcsBucket", temporaryGcsBucket)\
    .option("dataset", dataset_name)\
    .mode("overwrite")\
    .save("ratings")

print("Done")