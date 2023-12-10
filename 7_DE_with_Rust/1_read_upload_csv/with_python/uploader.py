import os
import time
from google.cloud import storage, bigquery
import pandas as pd
import polars as pl
from io import StringIO, BytesIO

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
        # Read the CSV file and select the first 6 columns
        df = pd.read_csv(source_file_name, usecols=range(6))
        df = df.head(10)

        # Convert the DataFrame to a CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Convert the CSV in memory to a binary stream
        binary_stream = BytesIO()
        binary_stream.write(csv_buffer.getvalue().encode())
        binary_stream.seek(0)

        client = bigquery.Client()
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        # Load data from the BytesIO object
        job = client.load_table_from_file(binary_stream, table_ref, job_config=job_config)

        job.result()
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

# Set variables
source_file_name = "/home/vytautas/Desktop/alban_news.csv"

# # test with pandas
# start = time.time()
# File.read_csv_with_pandas(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/1_read_upload_csv/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Pandas: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# # test with polars
# start = time.time()
# File.read_csv_with_polars(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/1_read_upload_csv/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Polars: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# test with BigQuery
start = time.time()
File.upload_to_bigquery(source_file_name, "data_tests", "alban_news")
end = time.time()
# export time to txt file
with open("7_DE_with_Rust/1_read_upload_csv/with_python/times.txt", "a") as f:
    f.write(f"Time elapsed with Py BigQuery: {(end - start)} seconds to upload {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")