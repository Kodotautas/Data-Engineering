import io
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from pydantic import BaseModel
from typing import List, Union


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

    def read_file_from_gcs(self) -> pd.DataFrame:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.config.bucket_name)
        blob = bucket.blob(f'{self.config.folder_name}/{self.config.file_name}')
        file_contents = blob.download_as_string()
        logging.info(f'Read {self.config.file_name} from {self.config.bucket_name}')
        return pd.read_csv(io.BytesIO(file_contents))

    def upload_file_to_bigquery(self, dataframe: pd.DataFrame):
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(self.config.dataset_name)
        table_ref = dataset_ref.table(self.config.table_name)

        table = bigquery.Table(table_ref, schema=self.get_schema())

        table = bigquery_client.create_table(table, exists_ok=True)

        dataframe.to_gbq(
            f'{self.config.dataset_name}.{self.config.table_name}',
            project_id=bigquery_client.project,
            if_exists='replace'
        )

        logging.info(f'Uploaded {table.num_rows} rows to {self.config.dataset_name}.{self.config.table_name}')
        print(f'Uploaded {table.num_rows} rows to {self.config.dataset_name}.{self.config.table_name}')

    def get_schema(self) -> List[bigquery.SchemaField]:
        schema = []
        for field in self.config.table_schema:
            schema.append(bigquery.SchemaField(field.name, field.data_type))
        return schema


# ----------------------------------- MAIN ----------------------------------- #

if __name__ == "__main__":
    schema_definitions = [
        TableSchema(name='MARKE', data_type='STRING'),
        TableSchema(name='KOMERCINIS_PAV', data_type='STRING'),
        TableSchema(name='KATEGORIJA_KLASE', data_type='STRING'),
        TableSchema(name='NUOSAVA_MASE', data_type='INTEGER'),
        TableSchema(name='GALIA', data_type='INTEGER'),
        TableSchema(name='GALIA_ELEKTR', data_type='INTEGER'),
        TableSchema(name='DEGALAI', data_type='STRING'),
        TableSchema(name='CO2_KIEKIS', data_type='INTEGER'),
        TableSchema(name='CO2_KIEKIS__WLTP', data_type='INTEGER'),
        TableSchema(name='TERSALU_LYGIS', data_type='STRING'),
        TableSchema(name='GALIOS_MASES_SANT', data_type='FLOAT'),
        TableSchema(name='PIRM_REG_DATA', data_type='STRING'),
        TableSchema(name='PIRM_REG_DATA_LT', data_type='STRING'),
        TableSchema(name='KODAS', data_type='STRING'),
        TableSchema(name='PAVADINIMAS', data_type='STRING'),
        TableSchema(name='SAVIVALDYBE', data_type='STRING'),
        TableSchema(name='APSKRITIS', data_type='STRING'),
    ]

    config = UploadConfig(
        bucket_name="lithuania_statistics",
        folder_name="companies_cars",
        file_name="Atviri_JTP_parko_duomenys.csv",
        dataset_name="lithuania_statistics",
        table_name="companies_cars",
        table_schema=schema_definitions
    )

    uploader = UploadToBigQuery(config)
    data_frame = uploader.read_file_from_gcs()
    uploader.upload_file_to_bigquery(data_frame)