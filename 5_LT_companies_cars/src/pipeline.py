# pipeline with gcp dataflow to download, save and upload data to bigquery
import datetime as dt
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

from src.mappings import file_configurations #!!!add src.mappings
from src.get_save_data import FinalUploader #!!!add src.get_save_data
from src.upload_to_bigquery import UploadConfig, UploadToBigQuery #!!!add src.upload_to_bigquery

# Configuration
current_year = dt.date.today().year
project = "vl-data-learn"
bucket_name = "lithuania_statistics"
region = "europe-west1"
staging_location = f"gs://{bucket_name}/staging"
temp_location = f"gs://{bucket_name}/temp"


# --------------------------------- PIPELINE --------------------------------- #
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

            # Download, save and upload the files
            (pipeline
                | f"Download {file_name}" >> beam.Create([file_url])
                | f"Upload {file_name}" >> beam.ParDo(FinalUploader())
            )

            # Get the file configuration.
            upload_config = UploadConfig(
                bucket_name = bucket_name,
                folder_name = "companies_cars",
                file_name = file_name,
                dataset_name = "lithuania_statistics",
                table_name = file_configuration["table_name"],
                table_schema = file_configuration["table_schema"]
            )

            # Initialize the uploader.
            uploader = UploadToBigQuery(upload_config)

            # Read the file from GCS.
            data_frame = uploader.read_file_from_gcs()

            # Upload the file to BigQuery.
            uploader.upload_file_to_bigquery(data_frame)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()