import pandas as pd
import xml.etree.ElementTree as ET
import requests
import logging
from pydantic import BaseModel, validator, HttpUrl
from typing import List
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

def url():
    '''Return URL to download data'''
    return 'https://osp-rs.stat.gov.lt/rest_xml/data/S3R168_M3010101_1'

url = url()

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
    '''Get data from URL and return data as XML'''
    response = requests.get(url.url)  # Extract the URL string from the URL object
    response.raise_for_status()  # Raise an exception if the request fails
    logging.info(f'Response status code: {response.status_code}')
    xml_data = response.text
    return xml_data

def parse_data(xml_data: str) -> Data:
    '''Parse XML data and return data as Data object'''
    root = ET.fromstring(xml_data)
    namespaces = {'g': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'}
    observations = root.findall('.//g:Obs', namespaces)
    rows = [Row(id=obs.find('g:ObsKey/g:Value', namespaces).attrib['id'],
                value=obs.find('g:ObsKey/g:Value', namespaces).attrib['value'])
            for obs in observations]
    data = Data(rows=rows)
    return data

class GCP(BaseModel):
    bucket_name: str
    filename: str

def save_data_to_gcp(df: pd.DataFrame, gcp: GCP):
    '''Save DataFrame to Google Cloud Storage'''
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcp.bucket_name)
    blob = bucket.blob(gcp.filename)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logging.info(f'Data saved to gs://{gcp.bucket_name}/{gcp.filename}')

# def main():
#     xml_data = get_data(URL(url=url))
#     data = parse_data(xml_data)
#     df = pd.DataFrame([row.dict() for row in data.rows])
#     gcp = GCP(bucket_name='lithuania_statistics', filename='lithuania_monthly_population.csv')
#     save_data_to_gcp(df, gcp)