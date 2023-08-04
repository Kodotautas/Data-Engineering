import argparse
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
df = df.head(3)

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
    """A function that saves data to a Google Cloud Storage bucket as an XML file."""
    logging.info(f'Saving data to gs://{bucket_name}/{filename}')
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{filename}')
    blob.upload_from_string(element, content_type='application/xml')
    logging.info(f'Data saved to gs://{bucket_name}/{filename}')


def run(base_url=base_url,
        project='vl-data-learn',
        region='europe-west1',
        staging_location='gs://lithuania_statistics/staging',
        temp_location='gs://lithuania_statistics/temp'):
    
    """A function that runs the Dataflow pipeline."""
    
    logging.getLogger().setLevel(logging.INFO)

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(GoogleCloudOptions).project = project
    options.view_as(GoogleCloudOptions).region = region
    options.view_as(GoogleCloudOptions).job_name = 'lithuania-statistics-population'
    options.view_as(GoogleCloudOptions).staging_location = staging_location
    options.view_as(GoogleCloudOptions).temp_location = temp_location

    with beam.Pipeline(options=options) as pipeline:
        # Loop through the DataFrame 'df' and process each row
        for idx, row in df.iterrows():
            url = base_url + row['id']
            folder_name = row['name']
            file_name = row['description']

            logging.info(f'Processing {url}')

            (pipeline
                | f'Create URL {idx}' >> beam.Create([url])
                | f'Download and process data {idx}' >> beam.Map(download_data)
                | f'Save data to GCS {idx}' >> beam.Map(save_data_to_gcs, 'lithuania_statistics', folder_name, file_name)
            )


# # --------------------------------- MAIN --------------------------------- #
if __name__ == '__main__':
    run()