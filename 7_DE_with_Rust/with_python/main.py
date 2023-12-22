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
    
    def read_csv_with_pandas_and_remove_nulls(file_name):
        """Reads a csv file with pandas and removes nulls."""
        df = pd.read_csv(file_name)
        df = df.dropna()
        return df
    
    def read_csv_with_pandas_filter_event_column(file_name):
        """Reads a csv file with pandas and count all 'Blitz' events in the Event column."""
        df = pd.read_csv(file_name)
        df = df[df['Event'] == " Blitz "]
        return print(df.shape[0])
    
    def read_csv_with_polars(file_name):
        """Reads a csv file with polars."""
        df = pl.read_csv(file_name)
        return df
    
    def read_csv_with_polars_and_remove_nulls(file_name):
        """Reads a csv file with polars and removes nulls."""
        df = pl.read_csv(file_name)
        df = df.drop_nulls()
        return df
    
    def read_csv_with_polars_filter_event_column(file_name):
        """Reads a csv file with polars and count all 'Blitz' events in the Event column."""
        df = pl.read_csv(file_name)
        df = df.filter(pl.col('Event').str.strip() == "Blitz")
        return print(df.shape[0])
    
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

    def read_from_bigquery(dataset_id, table_id, filename):
        """Reads a csv file from BigQuery and exports the first 13000 rows to a text file."""
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        rows = client.list_rows(table_ref, max_results=13000).to_dataframe()

        # Save DataFrame to a text file
        rows.to_csv(filename, sep='\t', index=False)

        return rows

# Set variables
source_file_name = "/home/vytautas/Desktop/chess_games.csv"


# test with pandas
# start = time.time()
# File.read_csv_with_pandas(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Pandas: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# # test with pandas and removing nulls
# start = time.time()
# File.read_csv_with_pandas_and_remove_nulls(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Pandas and removing nulls: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# test with pandas and filtering event column
# start = time.time()
# File.read_csv_with_pandas_filter_event_column(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Pandas and filtering event column: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# # test with polars
# start = time.time()
# File.read_csv_with_polars(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Polars: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# # test with polars and removing nulls
# start = time.time()
# File.read_csv_with_polars_and_remove_nulls(source_file_name)
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py Polars and removing nulls: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# test with polars and filtering event column
start = time.time()
File.read_csv_with_polars_filter_event_column(source_file_name)
end = time.time()
# export time to txt file
with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
    f.write(f"Time elapsed with Py Polars and filtering event column: {(end - start)} seconds to read {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")


# test with BigQuery
# start = time.time()
# File.upload_to_bigquery(source_file_name, "data_tests", "chess_games")
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Py BigQuery: {(end - start)} seconds to upload {source_file_name} which size is {os.path.getsize(source_file_name)} bytes.\n")

# # test with BigQuery
# start = time.time()
# File.read_from_bigquery("data_tests", "chess_games", "chess_games_from_bigquery.txt")
# end = time.time()
# # export time to txt file
# with open("7_DE_with_Rust/with_python/times.txt", "a") as f:
#     f.write(f"Time elapsed with Python BigQuery: {(end - start)} seconds to read table from BigQuery and save as .txt.\n")