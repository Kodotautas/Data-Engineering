import datetime as dt
import os
import pandas as pd
import io
import logging
import zipfile
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam import DoFn
import requests
from google.cloud import storage, bigquery
from pydantic import BaseModel
from typing import List

from src.mappings import file_configurations 

# Configuration
bucket_name = "lithuania_statistics"
staging_location = f"gs://{bucket_name}/staging"
temp_location = f"gs://{bucket_name}/temp"

# Data classes
class TableSchema(BaseModel):
    name: str
    data_type: str

class UploadConfig(BaseModel):
    bucket_name: str
    file_name: str
    table_name: str
    table_schema: List[TableSchema]


class DownloadSave(beam.DoFn):
    def __init__(self):
        self.current_year = dt.date.today().year
        self.file_configurations = file_configurations

    def download_file(self, url):
        """Download a file from the web and save it to GCS."""
        # Check if 'sodra' is present in the URL
        if 'sodra' in url:
            logging.info(f'Skipping download for URL containing "sodra": {url}')
            return None

        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
        local_filename = url.split('/')[-1].replace("-", "_")
        with requests.get(url, headers=header) as r:
            try:
                with open(local_filename, 'wb') as f:
                    f.write(r.content)
                    logging.info(f'Downloaded {url} to {local_filename}, size in MB: {round(os.path.getsize(local_filename) / 1024 / 1024, 2)}')
                return local_filename
            except Exception as e:
                logging.error(f"Error downloading file: {str(e)}")
                return None
    
    def extract_zip_contents(self, zip_file):
        """Extract the contents of a zip file."""
        try:
            extracted_files = {}
            with open(zip_file, "rb") as zip_file_obj:
                with zipfile.ZipFile(zip_file_obj) as zip_ref:
                    for file_info in zip_ref.infolist():
                        file_name = file_info.filename
                        extracted_files[file_name] = zip_ref.read(file_info)
                        logging.info(f'Extracted {file_name}')
            return extracted_files
        except Exception as e:
            logging.error(f"Error extracting zip file, name: {zip_file}, error: {str(e)}")
            return {}

    def process(self, element):
        # Iterate over the file configurations.
        for file_configuration in self.file_configurations:
            # Get the file configuration.
            url = file_configuration["url"]
            local_filename = self.download_file(url)
            extracted_files = self.extract_zip_contents(local_filename)
            for file_name, file_contents in extracted_files.items():
                # Upload the file to GCS.
                bucket = storage.Client().bucket(bucket_name)
                blob = bucket.blob(f'{bucket_name}/{file_configuration["file_name"]}')
                blob.upload_from_string(file_contents, content_type='text/csv')
                logging.info(f'Uploaded {file_name} to {bucket_name}/{file_configuration["file_name"]}')
                # Delete the local file.
                os.remove(local_filename)

        #unzip all files in bucket endind with .zip
        for blob in storage.Client().list_blobs(bucket_name):
            if blob.name.endswith(".zip"):
                blob.download_to_filename("/tmp/temp.zip")
                with zipfile.ZipFile("/tmp/temp.zip", 'r') as zip_ref:
                    zip_ref.extractall("/tmp/")
                for file in os.listdir("/tmp/"):
                    if file.endswith(".csv"):
                        blob = bucket.blob(f'{bucket_name}/{file}')
                        blob.upload_from_filename(f"/tmp/{file}")
                        logging.info(f'Uploaded {file} to {bucket_name}/{file}')
                        os.remove(f"/tmp/{file}")
                os.remove("/tmp/temp.zip")
                
        #rename in bucket Sodra file
        for blob in storage.Client().list_blobs(bucket_name):
            if 'monthly' in blob.name and blob.name.endswith(".csv"):
                new_name = file_configuration["file_name"]
                new_blob = bucket.rename_blob(blob, f'{bucket_name}/{new_name}')
                logging.info(f'File {blob.name} has been renamed to {new_blob.name}')

        
class UploadToBigQuery(beam.DoFn):
    def __init__(self):
        self.file_configurations = file_configurations
        self.transform_functions = {
            "Atviri_JTP_parko_duomenys.csv": self.transform_data_companies,
            "Atviri_TP_parko_duomenys.csv": self.transform_data_individuals,
            "employees_salaries_raw.csv": self.transform_data_employees
        }

    def read_file_from_gcs(self, config):
        """Reads a file from GCS and returns a DataFrame."""
        bucket = storage.Client().bucket(config.bucket_name)
        blob = bucket.blob(f'{config.bucket_name}/{config.file_name}')
        file_bytes = blob.download_as_bytes()
        data_frame = pd.read_csv(io.BytesIO(file_bytes), sep=self.get_delimiter())
        logging.info(f'Read {config.file_name} from GCS')
        return self.transform_functions[config.file_name](data_frame)
    
    def get_delimiter(self):
        """Returns a delimiter for a file."""
        for file_configuration in file_configurations:
            if file_configuration["file_name"] == self.config.file_name:
                logging.info(f'Got delimiter for {self.config.file_name}')
                return file_configuration["delimiter"]
        logging.error(f"Could not find delimiter for {self.config.file_name}")
        return None
    
    def transform_data_companies(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """Transforms the DataFrame by selecting columns and dropping missing values."""
        columns_to_keep = ['MARKE', 'KOMERCINIS_PAV', 'KATEGORIJA_KLASE', 'NUOSAVA_MASE', 'GALIA', 'GALIA_ELEKTR', 'DEGALAI', 'CO2_KIEKIS', 'CO2_KIEKIS__WLTP', 'TERSALU_LYGIS', 'GALIOS_MASES_SANT', 'PIRM_REG_DATA', 'PIRM_REG_DATA_LT', 'KODAS', 'PAVADINIMAS', 'SAVIVALDYBE', 'APSKRITIS']
        data_frame = data_frame[columns_to_keep]
        data_frame = data_frame.dropna(subset=['KOMERCINIS_PAV'])
        # make column KODAS as first column
        data_frame = data_frame[['KODAS'] + [col for col in data_frame.columns if col != 'KODAS']]
        logging.info(f'Transformed {self.config.file_name}')
        return data_frame
    
    def transform_data_individuals(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """Transforms the DataFrame by selecting columns and dropping missing values."""
        columns_to_keep = ['MARKE', 'KOMERCINIS_PAV', 'KATEGORIJA_KLASE', 'NUOSAVA_MASE', 'DARBINIS_TURIS', 'GALIA', 'GALIA_ELEKTR', 'DEGALAI', 'CO2_KIEKIS', 'CO2_KIEKIS_WLTP', 'RIDA', 'PIRM_REG_DATA', 'PIRM_REG_DATA_LT', 'VALD_TIPAS', 'SAVIVALDYBE', 'APSKRITIS']
        data_frame = data_frame[columns_to_keep]
        data_frame = data_frame.dropna(subset=['KOMERCINIS_PAV'])
        # Filter rows where VALD_TIPAS is 'Fizinis'
        data_frame = data_frame[data_frame['VALD_TIPAS'] == 'Fizinis']
        logging.info(f'Transformed {self.config.file_name}')
        return data_frame
    
    def transform_data_employees(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """Transforms the DataFrame by selecting columns and dropping missing values."""
        columns_to_keep = ['Juridinių asmenų registro kodas (jarCode)', 'Pavadinimas (name)', 'Savivaldybė, kurioje registruota(municipality)', 'Ekonominės veiklos rūšies kodas(ecoActCode)', 'Ekonominės veiklos rūšies pavadinimas(ecoActName)', 'Mėnuo (month)', 'Vidutinis darbo užmokestis (avgWage)', 'Apdraustųjų skaičius (numInsured)']
        # Make a copy of the DataFrame
        data_frame = data_frame[columns_to_keep].copy()
        # convert month 202301 (int64) to 2023-01-01 (date)
        data_frame['Mėnuo (month)'] = pd.to_datetime(data_frame['Mėnuo (month)'], format='%Y%m')
        data_frame['periodas'] = data_frame['Mėnuo (month)'].dt.date
        # drop month column
        data_frame = data_frame.drop(columns=['Mėnuo (month)'])
        # rename columns without lithuanian alphabet
        data_frame = data_frame.rename(columns={
            "Juridinių asmenų registro kodas (jarCode)": "kodas",
            "Pavadinimas (name)": "pavadinimas",
            "Savivaldybė, kurioje registruota(municipality)": "savivaldybe",
            "Ekonominės veiklos rūšies kodas(ecoActCode)": "veiklos_kodas",
            "Ekonominės veiklos rūšies pavadinimas(ecoActName)": "veiklos_pavadinimas",
            "Vidutinis darbo užmokestis (avgWage)": "vidutinis_darbo_uzmokestis",
            "Apdraustųjų skaičius (numInsured)": "apdraustuju_skaicius"
        })
        logging.info(f'Transformed {self.config.file_name}')
        return data_frame

    def get_job_config(self) -> bigquery.LoadJobConfig:
        """Returns a LoadJobConfig for BigQuery table loading."""
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.schema = self.get_table_schema()
        job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
        logging.info(f'Got job config for {self.config.file_name}')
        return job_config

    def get_table_schema(self) -> List[bigquery.SchemaField]:
        """Returns a list of BigQuery table schema fields."""
        table_schema = []
        for field in self.config.table_schema:
            table_schema.append(bigquery.SchemaField(field.name, field.data_type))
        logging.info(f'Got table schema for {self.config.file_name}')
        return table_schema

    def upload_file_to_bigquery(self, config, data_frame, bigquery_client):
        """Upload data to a BigQuery table."""
        table = f'{bucket_name}.{config.table_name}'
        job_config = self.get_job_config()
        job = bigquery_client.load_table_from_dataframe(data_frame, table, job_config=job_config)
        job.result()
        logging.info(f'Uploaded {config.file_name} to {bucket_name}.{config.table_name}')

    def process(self, element):
        for file_configuration in self.file_configurations:
            config = UploadConfig(
                bucket_name=bucket_name,
                file_name=file_configuration["file_name"],
                table_name=file_configuration["table_name"],
                table_schema=file_configuration["table_schema"]
            )
            self.config = config
            data_frame = self.read_file_from_gcs(config)
            bigquery_client = bigquery.Client()
            self.upload_file_to_bigquery(config, data_frame, bigquery_client)

class DownloadUploadTractors(beam.DoFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.current_year_month = dt.date.today().strftime("%Y-%m-01")

    def generate_url(self):
        """Generate a URL for a given year and month."""
        return f'https://data.gov.lt/dataset/278/download/13577/Ratiniai_traktoriai%20{self.current_year_month}%2008.00.00.csv'

    def download_and_upload(self, url):
        """Download a file from the web and upload directly to BigQuery."""
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
        logging.info(f'Downloading {url}')
        with requests.get(url, headers=header) as r:
            logging.info(f'Downloaded {url}, size in MB: {round(len(r.content) / 1024 / 1024, 2)}')
            try:
                data_frame = pd.read_csv(io.BytesIO(r.content), sep=';')
                # rename columns and select only needed columns
                data_frame = data_frame.rename(columns={
                    "Markė": "make",
                    " Komercinis pavadinimas (modelis)": "model",
                    " Galia kW": "power_kW",
                    " Gamybos metai": "reg_date",
                    " Pirmoji reg.data.LT": "reg_date_lt",
                    " Rajonas": "municipality",
                })
                data_frame = data_frame[['make', 'model', 'power_kW', 'reg_date', 'reg_date_lt', 'municipality']]
                data_frame['year_month'] = self.current_year_month

                # upload to BigQuery
                table = f'{bucket_name}.tractors'
                job_config = bigquery.LoadJobConfig()
                job_config.schema = [
                    bigquery.SchemaField("make", "STRING"),
                    bigquery.SchemaField("model", "STRING"),
                    bigquery.SchemaField("power_kW", "FLOAT"),
                    bigquery.SchemaField("reg_date", "DATE"),
                    bigquery.SchemaField("reg_date_lt", "DATE"),
                    bigquery.SchemaField("municipality", "STRING"),
                    bigquery.SchemaField("year_month", "DATE")
                ]
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.autodetect = True
                job_config.write_disposition = bigquery.WriteDisposition().WRITE_APPEND
                job = bigquery.Client().load_table_from_dataframe(data_frame, table, job_config=job_config)
                job.result()
                logging.info(f'Uploaded tractors to {table}')
            except Exception as e:
                logging.error(f"Error downloading and uploading tractors: {str(e)}")
                return None
            
    def process(self, element):
        url = self.generate_url()
        self.download_and_upload(url)

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).runner = "DataflowRunner"
    pipeline_options.view_as(GoogleCloudOptions).project = "vl-data-learn"
    pipeline_options.view_as(GoogleCloudOptions).region = "europe-west1"
    pipeline_options.view_as(GoogleCloudOptions).staging_location = staging_location
    pipeline_options.view_as(GoogleCloudOptions).temp_location = temp_location


    with beam.Pipeline(options=pipeline_options) as p:
        # Download and save files from the web to GCS.
        download_save = (
            p
            | 'Create' >> beam.Create([None])
            | 'Download and save' >> beam.ParDo(DownloadSave())
        )
        # Upload files from GCS to BigQuery.
        upload_to_bigquery = (
            p
            | 'Create 2' >> beam.Create([None])
            | 'Upload to BigQuery' >> beam.ParDo(UploadToBigQuery())
        )

        # Download and upload Tractors data.
        download_upload_tractors = (
            p
            | 'Create 3' >> beam.Create([None])
            | 'Download and upload tractors' >> beam.ParDo(DownloadUploadTractors())
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()