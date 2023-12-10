import os
import time
from google.cloud import storage, bigquery
import pandas as pd
import polars as pl

class File:
    def read_csv_with_pandas(file_name):
        """Reads a csv file with pandas."""
        df = pd.read_csv(file_name)
        return df
    
    def read_csv_with_polars(file_name):
        """Reads a csv file with polars."""
        df = pl.read_csv(file_name)
        return df
    
    def upload_to_bigquery(source_file_name, dataset_id, table_id):
        """Uploads a csv file to BigQuery."""
        client = bigquery.Client()
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        with open(source_file_name, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

# Set variables
source_file_name = "/home/vytautas/Desktop/chess_games.csv"


# test with pandas
start = time.time()
File.read_csv_with_pandas(source_file_name)
end = time.time()
# export time to txt file
with open("7_DE_with_Rust/1_read_upload_csv/with_python/times.txt", "a") as f:
    f.write(f"Time elapsed with Py Pandas: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# test with polars
start = time.time()
File.read_csv_with_polars(source_file_name)
end = time.time()
# export time to txt file
with open("7_DE_with_Rust/1_read_upload_csv/with_python/times.txt", "a") as f:
    f.write(f"Time elapsed with Py Polars: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# test with BigQuery
start = time.time()
File.upload_to_bigquery(source_file_name, "data_tests", "alban_news")
end = time.time()
# export time to txt file
with open("7_DE_with_Rust/1_read_upload_csv/with_python/times.txt", "a") as f:
    f.write(f"Time elapsed with Py BigQuery: {(end - start)} seconds to upload {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")