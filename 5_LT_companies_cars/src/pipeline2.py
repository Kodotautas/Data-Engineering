import datetime as dt
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam import DoFn
from mappings import file_configurations #!!!add src.mappings
from get_save_data import FinalUploader #!!!add src.get_save_data
from bigquery_uploader import UploadConfig, UploadToBigQuery #!!!add src.upload_to_bigquery


# Configuration
current_year = dt.date.today().year
project = "vl-data-learn"
bucket_name = "lithuania_statistics"
region = "europe-west1"
staging_location = f"gs://{bucket_name}/staging"
temp_location = f"gs://{bucket_name}/temp"


# --------------------------------- PIPELINE --------------------------------- #
class DownloadSave(DoFn):
    """Download and save data to GCS."""
    def process(self, element):
        FinalUploader.main(f"https://atvira.sodra.lt/imones/downloads/{current_year}/monthly-{current_year}.csv.zip")
        FinalUploader.main("https://www.regitra.lt/atvduom/Atviri_JTP_parko_duomenys.zip")

class BigQueryUploader(DoFn):
    """Upload data to BigQuery."""
    def process(self, element):
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
        # Download and save data to GCS.
        download_save = (
            pipeline
            | "Download and save data to GCS" >> beam.Create([None])
            | beam.ParDo(DownloadSave())
        )

        # Upload data to BigQuery.
        upload = (
            pipeline
            | "Upload data to BigQuery" >> beam.Create([None])
            | beam.ParDo(BigQueryUploader())
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
