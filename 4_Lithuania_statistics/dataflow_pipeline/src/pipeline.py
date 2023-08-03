import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.cloud import storage
import pandas as pd
import logging
import requests
from src.id_parser import IdParser


# ---------------------------------- PARAMS ---------------------------------- #
base_url = 'https://osp-rs.stat.gov.lt/rest_xml/data/'


# # -------------------------------- ID's parser ------------------------------- #
df = IdParser.df_transform(IdParser.parse_xml_to_dataframe(IdParser.download_data('https://osp-rs.stat.gov.lt/rest_xml/dataflow/')))


# # --------------------------------- EXTRACTOR -------------------------------- #
def download_data(url):
    """A function that downloads data from a specified URL."""
    logging.info(f'Downloading data from {url}')
    response = requests.get(url)
    response.raise_for_status()
    logging.info(f'Response status code: {response.status_code}')
    return response.text

def get_file_name(url):
    return url.split('/')[-1]

def save_data_to_gcs(element, bucket_name, folder_name, filename):
    """A function that saves data to a Google Cloud Storage bucket as a CSV file."""
    df = pd.DataFrame.from_records(element)
    logging.info(f'Saving data to gs://{bucket_name}/{filename}')
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{filename}')
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logging.info(f'Data saved to gs://{bucket_name}/{filename}')


# # --------------------------------- PIPELINE --------------------------------- #
def run():
    logging.getLogger().setLevel(logging.INFO)

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(GoogleCloudOptions).project = 'vl-data-learn'
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(GoogleCloudOptions).job_name = 'lithuania-statistics-population'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://lithuania_statistics/staging'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://lithuania_statistics/temp'

    with beam.Pipeline(options=options) as pipeline:
        (pipeline
            | 'Download data' >> beam.Create(['https://osp-rs.stat.gov.lt/rest_xml/data/S3R168_M3010101_1'])
            | 'Save data to GCS' >> beam.Map(save_data_to_gcs, 'lithuania_statistics', 'population', 'population.csv')
        )

if __name__ == '__main__':
    run()
