import os
import time
from google.cloud import storage

class File:
    def read_csv_with_pandas(file_name):
        """Reads a csv file with pandas."""
        import pandas as pd
        df = pd.read_csv(file_name)
        return df
    
    def read_csv_with_polars(file_name):
        """Reads a csv file with polars."""
        import polars as pl
        df = pl.read_csv(file_name)
        return df

# Set variables
source_file_name = "/home/vytautas/Desktop/alban_news.csv"

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