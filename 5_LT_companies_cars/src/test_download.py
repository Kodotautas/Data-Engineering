# script downloads file from url
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam import DoFn
import requests
from google.cloud import storage, bigquery
import requests
import apache_beam as beam
import logging

bucket_name = "lithuania_statistics"
staging_location = f"gs://{bucket_name}/staging"
temp_location = f"gs://{bucket_name}/temp"
url = "https://atvira.sodra.lt/imones/downloads/2023/monthly-2023.csv.zip"

class DownloadFile(DoFn):
    def download_file(self, url):  
        local_filename = url.split('/')[-1]
        with requests.get(url) as r:
            with open(local_filename, 'wb') as f:
                f.write(r.content)
                logging.info(f"File {local_filename} downloaded")
        return local_filename

    def save_to_bucket(self, local_filename):
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(local_filename)
        blob.upload_from_filename(local_filename)
        logging.info(f"File {local_filename} uploaded to bucket")

    def process(self, element):
        local_filename = self.download_file(element)
        self.save_to_bucket(local_filename)
        yield local_filename

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
            | "Create" >> beam.Create([url])
            | "Download" >> beam.ParDo(DownloadFile())
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run()