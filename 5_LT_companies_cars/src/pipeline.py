import datetime as dt
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from mappings import file_configurations
from get_save_data import main as get_save_data
from upload_to_bigquery import main as upload_to_bigquery


# --------------------------------- PIPELINE --------------------------------- #
def run():
    #set up logging
    logging.getLogger().setLevel(logging.INFO)

    #set up pipeline options
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True

    #create a pipeline
    with beam.Pipeline(options=options) as p:
        #create a PCollection from a list
        file_configurations = p | beam.Create(file_configurations)

        #get and save data
        get_save_data = file_configurations | beam.Map(get_save_data)

        #upload data to BigQuery
        upload_to_bigquery = file_configurations | beam.Map(upload_to_bigquery)

if __name__ == '__main__':
    run()

