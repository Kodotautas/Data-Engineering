import pandas as pd
import requests
import io
import logging
from google.cloud import bigquery


def download_csv(url):
    """Download a CSV file from the web."""
    try:
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
        logging.info(f'Downloading {url}')
        response = requests.get(url, headers=header)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"Error downloading CSV: {str(e)}")
        return None

def process_csv(csv_data):
    try:
        data = []
        for line in csv_data.split('\n'):
            line = line.strip()
            if len(line.split(';')) == 9:  # Check if the line has the expected number of columns
                data.append(line)
        
        data_frame = pd.read_csv(io.StringIO('\n'.join(data)), sep=';')
        data_frame = data_frame.rename(columns={
            "Markė": "make",
            "Komercinis pavadinimas (modelis)": "model",
            "Galia kW": "power_kW",
            "Pirmoji registracijos data": "reg_date",
            "Pirmoji reg.data.LT": "reg_date_lt",
            "Rajonas": "municipality",
            "Ivykusios registracijos metai": "reg_date_lt_year",
            "Ivykusios registracijos mėnuo": "reg_date_lt_month",
        })
        data_frame = data_frame[['make', 'model', 'power_kW', 'reg_date', 'reg_date_lt', 'municipality', 'reg_date_lt_year', 'reg_date_lt_month']]
        
        return data_frame
    except Exception as e:
        logging.error(f"Error processing CSV: {str(e)}")
        return None

def upload_to_bigquery(data_frame, table_name):
    try:
        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.schema = [
            bigquery.SchemaField("make", "STRING"),
            bigquery.SchemaField("model", "STRING"),
            bigquery.SchemaField("power_kW", "FLOAT"),
            bigquery.SchemaField("reg_date", "DATE"),
            bigquery.SchemaField("reg_date_lt", "DATE"),
            bigquery.SchemaField("municipality", "STRING"),
            bigquery.SchemaField("reg_date_lt_year", "INTEGER"),
            bigquery.SchemaField("reg_date_lt_month", "INTEGER"),
            bigquery.SchemaField("year_month", "DATE")
        ]
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = False
        job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
        job = bigquery.Client().load_table_from_dataframe(data_frame, table_name, job_config=job_config)
        job.result()
        logging.info(f'Uploaded data to {table_name}')
    except Exception as e:
        logging.error(f"Error uploading data to BigQuery: {str(e)}")

def main():
    # Set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    url = 'https://data.gov.lt/dataset/278/download/1118/Traktoriai_2020-01-15.csv'
    table_name = 'lithuania_statistics.tractors_2009_2019'
    
    csv_data = download_csv(url)
    if csv_data:
        data_frame = process_csv(csv_data)
        if data_frame is not None:
            upload_to_bigquery(data_frame, table_name)

if __name__ == '__main__':
    main()