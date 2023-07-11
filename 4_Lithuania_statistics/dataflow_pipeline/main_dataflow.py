import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import logging

class DownloadData(beam.DoFn):
    def __init__(self, url):
        self.url = url

    def process(self, element):
        # Download data from the specified URL
        logging.info(f'Downloading data from {self.url}')
        response = requests.get(self.url)
        response.raise_for_status()
        logging.info(f'Response status code: {response.status_code}')
        xml_data = response.text
        yield xml_data

class ParseData(beam.DoFn):
    def process(self, element):
        # Parse the XML data and extract the relevant information
        logging.info('Parsing XML data')
        root = ET.fromstring(element)
        namespaces = {'g': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'}
        observations = root.findall('.//g:Obs', namespaces)
        rows = [{'id': obs.find('g:ObsKey/g:Value', namespaces).attrib['id'],
                 'value': obs.find('g:ObsKey/g:Value', namespaces).attrib['value']}
                for obs in observations]
        yield rows

class SaveToGCS(beam.DoFn):
    def __init__(self, bucket_name, filename):
        self.bucket_name = bucket_name
        self.filename = filename

    def process(self, element):
        # Save the data to a Google Cloud Storage bucket as a CSV file
        logging.info(f'Saving data to gs://{self.bucket_name}/{self.filename}')
        df = pd.DataFrame(element)
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.filename)
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
        logging.info(f'Data saved to gs://{self.bucket_name}/{self.filename}')

def run():
    # Set the URL to download data from and the GCS bucket and filename to save the data to
    url = 'https://osp-rs.stat.gov.lt/rest_xml/data/S3R168_M3010101_1'
    bucket_name = 'lithuania_statistics'
    filename = 'lithuania_monthly_population.csv'

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        data = (
            p
            | 'Create' >> beam.Create([None])
            | 'Download Data' >> beam.ParDo(DownloadData(url))
            | 'Parse Data' >> beam.ParDo(ParseData())
            | 'Save to GCS' >> beam.ParDo(SaveToGCS(bucket_name, filename))
        )

if __name__ == '__main__':
    run()
