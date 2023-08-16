# pipeline with gcp dataflow to download, save and upload data to bigquery
import datetime as dt
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from mappings import file_configurations
from get_save_data import FinalUploader
from upload_to_bigquery import UploadToBigQuery


# --------------------------------- PIPELINE --------------------------------- #
class DownloadSaveData(beam.DoFn):
    def process(self, element):
        FinalUploader.main(element)
        UploadToBigQuery.main()

def run():
    # Set the pipeline options.
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True

    # Initialize the pipeline.
    with beam.Pipeline(options=options) as pipeline:
        # Iterate over the file configurations.
        for file_configuration in file_configurations:
            # Get the file configuration.
            file_name = file_configuration["file_name"]
            if file_name == "employees_salaries_raw.csv":
                file_url = f"https://atvira.sodra.lt/imones/downloads/{dt.date.today().year}/monthly-{dt.date.today().year}.csv.zip"

            # Download the file.
            (pipeline
                | f"Create {file_name} URL" >> beam.Create([file_url])
                | f"Download {file_name}" >> beam.ParDo(DownloadSaveData())
            )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run()