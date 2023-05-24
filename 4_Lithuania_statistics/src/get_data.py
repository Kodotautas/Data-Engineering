import pandas as pd
import xml.etree.ElementTree as ET
import requests
from google.cloud import storage
import logging
from pydantic import BaseModel, validator, HttpUrl
from typing import List

client = storage.Client()

data_code = 'S3R168_M3010101_1'
url = f'https://osp-rs.stat.gov.lt/rest_xml/data/{data_code}'

class Row(BaseModel):
    id: str
    value: str

class Data(BaseModel):
    rows: List[Row]

class URL(BaseModel):
    url: HttpUrl

    @validator('url')
    def url_must_be_https(cls, v):
        if not v.startswith('https'):
            raise ValueError('URL must be https')
        return v

def get_data(url: URL) -> str:
    '''Get data from url and return data as xml'''
    response = requests.get(url)
    logging.info(f'Response status code: {response.status_code}')
    xml_data = response.text
    return xml_data

def parse_xml(xml_data: str) -> Data:
    root = ET.fromstring(xml_data)
    data = []
    for obs in root.findall('.//Obs'):
        row = {}
        for value in obs.findall('.//Value'):
            row[value.get('id')] = value.get('value')
        row['value'] = obs.find('.//ObsValue').get('value')
        data.append(Row(**row))
    return Data(rows=data)

class GCP(BaseModel):
    bucket_name: str
    filename: str

def save_data_to_gcp(df: pd.DataFrame, gcp: GCP):
    df.to_csv(f'gs://{gcp.bucket_name}/{gcp.filename}', index=False)
    logging.info(f'Data saved to gs://{gcp.bucket_name}/{gcp.filename}')

def main():
    logging.basicConfig(level=logging.INFO)
    xml_data = get_data(url)
    data = parse_xml(xml_data)
    df = pd.DataFrame(data.rows)
    print(df.head())
    gcp = GCP(bucket_name='lithuania_statistics', filename=f'{data_code}.csv')
    save_data_to_gcp(df, gcp)

if __name__ == '__main__':
    main()