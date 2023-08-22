# pipeline with gcp dataflow to download, save and upload data to bigquery
import datetime as dt
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

from mappings import file_configurations #!!!add src.mappings
from get_save_data import FinalUploader #!!!add src.get_save_data
from upload_to_bigquery import UploadToBigQuery #!!!add src.upload_to_bigquery

# Configuration
current_year = dt.date.today().year
project = "vl-data-learn"
bucket_name = "lithuania_statistics"
region = "europe-west1"
staging_location = f"gs://{project}/staging"
temp_location = f"gs://{project}/temp"


# --------------------------------- PIPELINE --------------------------------- #
class DownloadSaveData(beam.DoFn):
    def process(self, element):
        FinalUploader.main(element)
        UploadToBigQuery.main()


def run():
    # Set the pipeline options.
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = "DataflowRunner"
    options.view_as(GoogleCloudOptions).project = project
    options.view_as(GoogleCloudOptions).region = region
    options.view_as(GoogleCloudOptions).staging_location = staging_location
    options.view_as(GoogleCloudOptions).temp_location = temp_location
    options.view_as(GoogleCloudOptions).job_name = f"lithuania-statistics-cars"

    # Initialize the pipeline.
    with beam.Pipeline(options=options) as pipeline:
        # Iterate over the file configurations.
        for file_configuration in file_configurations:
            # Get the file configuration.
            file_name = file_configuration["file_name"]
            file_url = file_configuration["url"]
            if file_name == "employees_salaries_raw.csv":
                file_url = f"https://atvira.sodra.lt/imones/downloads/{current_year}/monthly-{current_year}.csv.zip"

            # Download and save the file.
            (pipeline
                | f"Create {file_name} URL" >> beam.Create([file_url])
                | f"Download {file_name}" >> beam.ParDo(DownloadSaveData())
            )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run()