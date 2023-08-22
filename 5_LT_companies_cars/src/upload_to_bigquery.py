import io
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from pydantic import BaseModel
from typing import List

# Parameters
from mappings import file_configurations #!!!!add src.mappings

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
        self.transform_functions = {
            "Atviri_JTP_parko_duomenys.csv": self.transform_data_companies,
            "employees_salaries_raw.csv": self.transform_data_employees
        }

    def read_file_from_gcs(self) -> pd.DataFrame:
        """Reads a .csv file row by row and if error skips the row, logs the error."""
        bucket = self.storage_client.bucket(self.config.bucket_name)
        blob = bucket.blob(f'{self.config.folder_name}/{self.config.file_name}')
        blob_as_string = blob.download_as_string()
        delimiter = self.get_delimiter()
        data_frame = pd.read_csv(io.BytesIO(blob_as_string), sep=delimiter, on_bad_lines='skip', encoding='utf-8')
        # Get the transform function based on the table name
        transform_function = self.transform_functions[self.config.file_name]
        data_frame = transform_function(data_frame)
        return data_frame
    
    def get_delimiter(self) -> str:
        """get delimiter based on file name from mappings.py"""
        for file_configuration in file_configurations:
            if file_configuration["file_name"] == self.config.file_name:
                return file_configuration["delimiter"]
        logging.error(f"Could not find delimiter for {self.config.file_name}")
        return None

    # -------------------------------- TRANSFORMS -------------------------------- #
    def transform_data_companies(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """Transforms the DataFrame by selecting columns and dropping missing values."""
        columns_to_keep = ['MARKE', 'KOMERCINIS_PAV', 'KATEGORIJA_KLASE', 'NUOSAVA_MASE', 'GALIA', 'GALIA_ELEKTR', 'DEGALAI', 'CO2_KIEKIS', 'CO2_KIEKIS__WLTP', 'TERSALU_LYGIS', 'GALIOS_MASES_SANT', 'PIRM_REG_DATA', 'PIRM_REG_DATA_LT', 'KODAS', 'PAVADINIMAS', 'SAVIVALDYBE', 'APSKRITIS']
        data_frame = data_frame[columns_to_keep]
        data_frame = data_frame.dropna(subset=['KOMERCINIS_PAV'])
        # make column KODAS as first column
        data_frame = data_frame[['KODAS'] + [col for col in data_frame.columns if col != 'KODAS']]
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

    def main():
        # Initialize the GCS client.
        storage_client = storage.Client()

        # Initialize the BigQuery client.
        bigquery_client = bigquery.Client()

        # Iterate over the file configurations.
        for file_configuration in file_configurations:
            # Get the file configuration.
            upload_config = UploadConfig(
                bucket_name = "lithuania_statistics",
                folder_name = "companies_cars",
                file_name = file_configuration["file_name"],
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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    UploadToBigQuery.main()