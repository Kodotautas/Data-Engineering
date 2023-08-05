import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
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


class ProcessData(beam.DoFn):
    def process(self, element):
        url, idx, folder_name, file_name = element
        logging.info(f'Processing {url}')
        data = requests.get(url).text
        yield (folder_name, file_name, data)


class SaveDataToGCS(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, element):
        folder_name, file_name, data = element
        logging.info(f'Saving data to gs://{self.bucket_name}/{folder_name}/{file_name}')
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(f'{folder_name}/{file_name}')
        blob.upload_from_string(data, content_type='application/xml')
        logging.info(f'Data saved to gs://{self.bucket_name}/{folder_name}/{file_name}')


def run(base_url=base_url,
        project='vl-data-learn',
        region='europe-west1',
        staging_location='gs://lithuania_statistics/staging',
        temp_location='gs://lithuania_statistics/temp',
        output_bucket='lithuania_statistics'):
    
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
        (pipeline
            | 'Create URL' >> beam.Create([(base_url + row['id'], idx, row['name'], row['description']) for idx, row in df.iterrows()])
            | 'Process Data' >> beam.ParDo(ProcessData())
            | 'Save data to GCS' >> beam.ParDo(SaveDataToGCS(output_bucket))
        )


# # --------------------------------- MAIN --------------------------------- #
if __name__ == '__main__':
    run()