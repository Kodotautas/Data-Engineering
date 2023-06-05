# create and run pipeline with dataflow
import argparse
import logging
import apache_beam as beam
from src.save_data import get_data, parse_data, save_data_to_gcp, url

logging.basicConfig(level=logging.INFO)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', dest='url', required=True, help='URL to download data')
    parser.add_argument('--bucket_name', dest='bucket_name', required=True, help='GCS bucket name')
    parser.add_argument('--filename', dest='filename', required=True, help='GCS filename')
    known_args, pipeline_args = parser.parse_known_args(argv)
    with beam.Pipeline(argv=pipeline_args) as p:
        (p
         | 'Create URL' >> beam.Create([known_args.url])
         | 'Get data' >> beam.Map(get_data)
         | 'Parse data' >> beam.Map(parse_data)
         | 'Save data to GCS' >> beam.Map(save_data_to_gcp, known_args.bucket_name, known_args.filename)
         )
        
if __name__ == '__main__':
    run()