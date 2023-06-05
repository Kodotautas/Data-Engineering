import os
import sys

from pyspark.sql import SparkSession
from google.cloud import bigquery
from math import ceil
from pyspark.sql.functions import col, udf

# Required if at some point you got 
# 'java.io.IOException: Cannot run program "python3": CreateProcess error=2, The system cannot find the file specified'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Build the spark session
spark = SparkSession.builder.appName("Movies ETL - movies").getOrCreate()

# Load data file from cloud storage
movies_file = "gs://course_data_engineering_sample_data/movie-datasets/IMDb_movies.gz"
movies_spark = spark.read.csv(movies_file, header=True)

# Transformation sample on spark dataframe
movies_spark = movies_spark.na.fill({
    "metascore" : "0",
    "usa_gross_income" : "$ 0", 
    "budget" : "$ 0", 
    "worlwide_gross_income" : "$ 0", 
    "reviews_from_critics" : "0", 
    "reviews_from_users" : "0",
    "production_company" : "Unknown", 
    "writer" : "Unknown",
    "language" : "Unknown", 
    "director" : "Unknown",
    "actors" : "Unknown", 
    "country" : "Unknown"
})

# UDF sample on spark dataframe
exchange_rate_to_usd = {
    "GBP" : 1.39, "INR" : 0.013, "EUR" : 1.19, "AUD" : 0.73, "KRW" : 0.00087, 
    "BRL" : 0.19, "CAD" : 0.8, "NOK" : 0.11, "CZK" : 0.047, "PLN" : 0.26, 
    "HKD" : 0.13, "CLP" : 0.0013, "CNY" : 0.15, "SEK" : 0.12, "NZD" : 0.7, 
    "NGN" : 0.0024, "DKK" : 0.16, "ARS" : 0.01, "HUF" : 0.0033, "MXN" : 0.05, 
    "JPY" : 0.0091, "ILS" : 0.31, "DOP" : 0.018, "ISK" : 0.0081, "DEM" : 0.61, 
    "FRF" : 0.18, "RUR" : 0.014, "ITL" : 0.00061, "BEF" : 0.029, "ESP" : 0.0071, 
    "ATS" : 0.086, "THB" : 0.030, "FIM" : 0.2, "SGD" : 0.74, "CHF" : 1.11
}

def convert_to_usd_string(amount_string="$ 0"):
    try:
        if amount_string[0:1] != "$":
            # Default to 1 if no exchange rate found (demo)
            rate = exchange_rate_to_usd.get(amount_string[:3], 1)
            amount_usd = int(amount_string[4:]) * rate
            return "$ " + str(ceil(amount_usd))
    except:
        return "$ 0"
    
    return amount_string

# Register the UDF
convert_to_usd_string_udf = spark.udf.register("convert_to_usd_string", convert_to_usd_string)

# Use the UDF to replace column value
movies_spark = movies_spark.withColumn("budget",  convert_to_usd_string_udf(col("budget")))
movies_spark = movies_spark.withColumn("usa_gross_income",  convert_to_usd_string_udf(col("usa_gross_income")))
movies_spark = movies_spark.withColumn("worlwide_gross_income",  convert_to_usd_string_udf(col("worlwide_gross_income")))

# Another UDF, this time using lambda
remove_dollar_sign_udf = udf( lambda usd_string: int(usd_string[1:]) )

# Use the UDF to replace column value
movies_spark = movies_spark.withColumn("budget",  remove_dollar_sign_udf(col("budget")))
movies_spark = movies_spark.withColumn("usa_gross_income",  remove_dollar_sign_udf(col("usa_gross_income")))
movies_spark = movies_spark.withColumn("worlwide_gross_income",  remove_dollar_sign_udf(col("worlwide_gross_income")))

# ...
# Other transformation skipped for simplicity
# ...

# TODO : replace google cloud project id with your own
project_id = "timpamungkas-udemy" 
# TODO : replace the 'temporaryGcsBucket' with your own google cloud storage bucket
temporaryGcsBucket = "course_data_engineering_spark_temp"

# create bigquery dataset
client = bigquery.Client()
dataset_name = "spark_dataset"
dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
dataset.location = "us-east1"
#dataset.default_table_expiration_ms = 3600000
dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)

# Write to bigquery
movies_spark.write.format("bigquery")\
    .option("temporaryGcsBucket", temporaryGcsBucket)\
    .option("dataset", dataset_name)\
    .mode("overwrite")\
    .save("movies")
	
print("Done")