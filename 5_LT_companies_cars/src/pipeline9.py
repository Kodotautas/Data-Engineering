import datetime as dt
import os
import random
import pandas as pd
import io
import logging
import zipfile
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam import DoFn
import requests
from google.cloud import storage, bigquery
from pydantic import BaseModel
from typing import List
from src.mappings import file_configurations 

# Configuration
bucket_name = "lithuania_statistics"
staging_location = f"gs://{bucket_name}/staging"
temp_location = f"gs://{bucket_name}/temp"

class TableSchema(BaseModel):
    name: str
    data_type: str

class UploadConfig(BaseModel):
    bucket_name: str
    folder_name: str
    file_name: str
    dataset_name: str
    table_name: str
    table_schema: List[TableSchema]

class DownloadSave(beam.DoFn):
    def __init__(self):
        self.current_year = dt.date.today().year
        self.file_configurations = file_configurations

    def download_file(self, url):
        # Check if 'sodra' is present in the URL
        if 'sodra' in url:
            logging.info(f'Skipping download for URL containing "sodra": {url}')
            return None

        header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0"
    }
        local_filename = url.split('/')[-1].replace("-", "_")
        with requests.get(url, headers=header) as r:
            try:
                with open(local_filename, 'wb') as f:
                    f.write(r.content)
                    logging.info(f'Downloaded {url} to {local_filename}, size in MB: {round(os.path.getsize(local_filename) / 1024 / 1024, 2)}')
                return local_filename
            except Exception as e:
                logging.error(f"Error downloading file: {str(e)}")
                return None
    
    def extract_zip_contents(self, zip_file):
        try:
            extracted_files = {}
            with open(zip_file, "rb") as zip_file_obj:
                with zipfile.ZipFile(zip_file_obj) as zip_ref:
                    for file_info in zip_ref.infolist():
                        file_name = file_info.filename
                        extracted_files[file_name] = zip_ref.read(file_info)
                        logging.info(f'Extracted {file_name}')
            return extracted_files
        except Exception as e:
            logging.error(f"Error extracting zip file, name: {zip_file}, error: {str(e)}")
            return {}

    def process(self, element):
        # Iterate over the file configurations.
        for file_configuration in self.file_configurations:
            # Get the file configuration.
            url = file_configuration["url"]
            local_filename = self.download_file(url)
            extracted_files = self.extract_zip_contents(local_filename)
            for file_name, file_contents in extracted_files.items():
                # Upload the file to GCS.
                bucket = storage.Client().bucket(bucket_name)
                blob = bucket.blob(f'{bucket_name}/{file_configuration["file_name"]}')
                blob.upload_from_string(file_contents, content_type='text/csv')
                logging.info(f'Uploaded {file_name} to {bucket_name}/{file_configuration["file_name"]}')
                # Delete the local file.
                os.remove(local_filename)

        #unzip all files in bucket endind with .zip
        for blob in storage.Client().list_blobs(bucket_name):
            if blob.name.endswith(".zip"):
                blob.download_to_filename("/tmp/temp.zip")
                with zipfile.ZipFile("/tmp/temp.zip", 'r') as zip_ref:
                    zip_ref.extractall("/tmp/")
                for file in os.listdir("/tmp/"):
                    if file.endswith(".csv"):
                        blob = bucket.blob(f'{bucket_name}/{file}')
                        blob.upload_from_filename(f"/tmp/{file}")
                        logging.info(f'Uploaded {file} to {bucket_name}/{file}')
                        os.remove(f"/tmp/{file}")
                os.remove("/tmp/temp.zip")
                
        #rename in bucket Sodra file
        for blob in storage.Client().list_blobs(bucket_name):
            if 'monthly' in blob.name and blob.name.endswith(".csv"):
                new_name = file_configuration["file_name"]
                new_blob = bucket.rename_blob(blob, f'{bucket_name}/{new_name}')
                logging.info(f'File {blob.name} has been renamed to {new_blob.name}')

class UploadToBigQuery(beam.DoFn):
    def __init__(self):
        # self.transform_functions = {
        #     "companies_cars.csv": self.transform_data_companies,
        #     "employees.csv": self.transform_data_employees
        # }
        self.file_configurations = file_configurations

    def read_file_from_gcs(self):
        bucket = self.storage_client.bucket(self.config.bucket_name)
        blob = bucket.blob(f'{self.config.folder_name}/{self.config.file_name}')
        file_bytes = blob.download_as_bytes()
        data_frame = pd.read_csv(io.BytesIO(file_bytes), sep=self.config.delimiter)
        logging.info(f'Read {self.config.file_name} from GCS')
        return self.transform_functions[self.config.file_name](data_frame)
    
    def process(self, element):
        # Initialize the clients.
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()
        # Iterate over the file configurations.
        for file_configuration in self.file_configurations:
            # Get the file configuration.
            self.config = UploadConfig(**file_configuration)
            # Read the file from GCS.
            data_frame = self.read_file_from_gcs()
            logging.info(f'File {data_frame} has been read from GCS')

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).runner = "DataflowRunner"
    pipeline_options.view_as(GoogleCloudOptions).project = "vl-data-learn"
    pipeline_options.view_as(GoogleCloudOptions).region = "europe-west1"
    pipeline_options.view_as(GoogleCloudOptions).staging_location = staging_location
    pipeline_options.view_as(GoogleCloudOptions).temp_location = temp_location

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Create" >> beam.Create([None])
            | "Download and save" >> beam.ParDo(DownloadSave())
            | "Upload to BigQuery" >> beam.ParDo(UploadToBigQuery())
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()