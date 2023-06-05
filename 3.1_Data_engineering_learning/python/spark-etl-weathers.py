import os
import sys
from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql.types import StructType, StructField, StringType, DateType
from random import randint, random

# optional
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("Weathers ETL").getOrCreate()

schema = StructType([
    StructField("station_id", StringType()),
    StructField("date_yyyymmdd", DateType()),
    StructField("observation_type", StringType()),
    StructField("observation_value", StringType()),
    StructField("observation_time_hhmm", StringType()),
])

weather_files = "gs://course_data_engineering_weather_data/*.csv.gz"
weather_spark = spark.read.csv(weather_files, header=False, schema=schema, dateFormat="yyyymmdd")

# TODO : replace google cloud project id with your own
project_id = "timpamungkas"
# TODO : replace the temporary gcs bucket with your own cloud storage bucket
temporary_gcs_bucket = "course_data_engineering_spark_temp"

client = bigquery.Client()
dataset_name = "spark_dataset"
dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
dataset.location = "us-east1"
dataset.default_table_expiration_ms = 3600000
dataset = client.create_dataset(dataset=dataset, exists_ok=True, timeout=30)

random_int = randint(1000, 9999)
weather_spark.write.format("bigquery")\
    .option("temporaryGcsBucket", temporary_gcs_bucket)\
    .option("dataset", dataset_name)\
    .mode("overwrite")\
    .save(f"weathers-{random_int}")