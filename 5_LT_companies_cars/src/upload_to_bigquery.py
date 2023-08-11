import io
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from pydantic import BaseModel
from typing import List

# Parameters
BUCKET_NAME = "lithuania_statistics"
FOLDER_NAME = "companies_cars"
FILE_NAME = "Atviri_JTP_parko_duomenys.csv"
DATASET_NAME = "lithuania_statistics"
TABLE_NAME = "companies_cars_raw"


# --------------------------------- MODELS --------------------------------- #
class TableSchema(BaseModel):
    name: str
    data_type: str


class UploadConfig(BaseModel):
    bucket_name: str
    folder_name: str
    file_name: str
    dataset_name: str
    table_name: str
    table_schema: List[TableSchema]


# --------------------------------- UPLOADER --------------------------------- #
class UploadToBigQuery:
    def __init__(self, config: UploadConfig):
        self.config = config
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()

    def read_file_from_gcs(self) -> pd.DataFrame:
        """Reads a .csv file row by row and if error skips the row and logs the error."""
        bucket = self.storage_client.bucket(self.config.bucket_name)
        blob = bucket.blob(f'{self.config.folder_name}/{self.config.file_name}')
        blob_string = blob.download_as_string()
        data_frame = pd.read_csv(io.BytesIO(blob_string))
        return self.transform_data(data_frame)

    def transform_data(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """Transforms the DataFrame by selecting columns and dropping missing values."""
        columns_to_keep = ['MARKE', 'KOMERCINIS_PAV', 'KATEGORIJA_KLASE', 'NUOSAVA_MASE', 'GALIA', 'GALIA_ELEKTR', 'DEGALAI', 'CO2_KIEKIS', 'CO2_KIEKIS__WLTP', 'TERSALU_LYGIS', 'GALIOS_MASES_SANT', 'PIRM_REG_DATA', 'PIRM_REG_DATA_LT', 'KODAS', 'PAVADINIMAS', 'SAVIVALDYBE', 'APSKRITIS']
        data_frame = data_frame[columns_to_keep]
        data_frame = data_frame.dropna(subset=['KOMERCINIS_PAV'])
        return data_frame

    def upload_file_to_bigquery(self, data_frame: pd.DataFrame):
        """Upload data to a BigQuery table."""
        dataset = self.bigquery_client.dataset(self.config.dataset_name)
        table = dataset.table(self.config.table_name)
        job_config = self.get_job_config()
        job = self.bigquery_client.load_table_from_dataframe(data_frame, table, job_config=job_config)
        job.result()
        logging.info(f'Uploaded {self.config.file_name} to {self.config.dataset_name}.{self.config.table_name}')

    def get_job_config(self) -> bigquery.LoadJobConfig:
        """Returns a LoadJobConfig for BigQuery table loading."""
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.schema = self.get_table_schema()
        job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
        return job_config

    def get_table_schema(self) -> List[bigquery.SchemaField]:
        """Returns a list of BigQuery table schema fields."""
        table_schema = []
        for field in self.config.table_schema:
            table_schema.append(bigquery.SchemaField(field.name, field.data_type))
        return table_schema


# ----------------------------------- MAIN ----------------------------------- #
if __name__ == "__main__":
    schema_definitions = [
        TableSchema(name='MARKE', data_type='STRING'),
        TableSchema(name='KOMERCINIS_PAV', data_type='STRING'),
        TableSchema(name='KATEGORIJA_KLASE', data_type='STRING'),
        TableSchema(name='NUOSAVA_MASE', data_type='FLOAT'),
        TableSchema(name='GALIA', data_type='FLOAT'),
        TableSchema(name='GALIA_ELEKTR', data_type='FLOAT'),
        TableSchema(name='DEGALAI', data_type='STRING'),
        TableSchema(name='CO2_KIEKIS', data_type='INTEGER'),
        TableSchema(name='CO2_KIEKIS__WLTP', data_type='INTEGER'),
        TableSchema(name='TERSALU_LYGIS', data_type='STRING'),
        TableSchema(name='GALIOS_MASES_SANT', data_type='FLOAT'),
        TableSchema(name='PIRM_REG_DATA', data_type='DATE'),
        TableSchema(name='PIRM_REG_DATA_LT', data_type='DATE'),
        TableSchema(name='KODAS', data_type='STRING'),
        TableSchema(name='PAVADINIMAS', data_type='STRING'),
        TableSchema(name='SAVIVALDYBE', data_type='STRING'),
        TableSchema(name='APSKRITIS', data_type='STRING'),
    ]

    config = UploadConfig(
        bucket_name=BUCKET_NAME,
        folder_name=FOLDER_NAME,
        file_name=FILE_NAME,
        dataset_name=DATASET_NAME,
        table_name=TABLE_NAME,
        table_schema=schema_definitions
    )

    uploader = UploadToBigQuery(config)
    data_frame = uploader.read_file_from_gcs()
    uploader.upload_file_to_bigquery(data_frame)