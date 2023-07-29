import src
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.cloud import storage
import pandas as pd
import logging
import re
import xml.etree.ElementTree as ET
import requests


# --------------------------------- FUNCTIONS -------------------------------- #
def download_data(element, url):
    """A function that downloads data from a specified URL."""
    logging.info(f'Downloading data from {url}')
    response = requests.get(url)
    response.raise_for_status()
    logging.info(f'Response status code: {response.status_code}')
    return response.text

def parse_data(element):
    """A function that parses XML data and extracts the relevant information."""
    logging.info('Parsing XML data')
    root = ET.fromstring(element)
    namespaces = {'g': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'}
    observations = root.findall('.//g:Obs', namespaces)
    rows = []
    for obs in observations:
        period = obs.find('g:ObsKey/g:Value[@id="LAIKOTARPIS"]', namespaces).attrib['value']
        population = obs.find('g:ObsValue', namespaces).attrib['value']

        # Transform the period from "2008M12" to "2008-12"
        period = re.sub(r'M(\d+)$', r'-\1', period)

        rows.append({'period': period, 'population': population})
    return rows

def save_data_to_gcs(element, bucket_name, filename):
    """A function that saves data to a Google Cloud Storage bucket as a CSV file."""
    df = pd.DataFrame(element)
    logging.info(f'Saving data to gs://{bucket_name}/{filename}')
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logging.info(f'Data saved to gs://{bucket_name}/{filename}')


# # --------------------------------- PIPELINE --------------------------------- #
def run():
    logging.getLogger().setLevel(logging.INFO)

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(GoogleCloudOptions).project = 'vl-data-learn'
    options.view_as(GoogleCloudOptions).region = 'europe-central2'  # Correct the region here
    options.view_as(GoogleCloudOptions).job_name = 'lithuania-statistics-population'
    options.view_as(GoogleCloudOptions).staging_location = 'gs://lithuania_statistics/staging'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://lithuania_statistics/temp'    

    with beam.Pipeline(options=options) as pipeline:
        (pipeline
            | 'Create' >> beam.Create([None])
            | 'Download Data' >> beam.ParDo(download_data, 'https://osp-rs.stat.gov.lt/rest_xml/data/S3R168_M3010101_1')
            | 'Parse Data' >> beam.Map(parse_data)
            | 'Save to GCS' >> beam.ParDo(save_data_to_gcs, 'lithuania_statistics', 'lithuania_monthly_population.csv')
        )

if __name__ == '__main__':
    run()
