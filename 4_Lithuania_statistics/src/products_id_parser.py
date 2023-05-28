import pandas as pd
import xml.etree.ElementTree as ET
import requests
import logging
from pydantic import BaseModel, validator, HttpUrl
from typing import List
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

url = 'https://osp-rs.stat.gov.lt/rest_xml/dataflow/'

def get_data(url):
    '''Get data from URL and return data as XML'''
    response = requests.get(url)  # Extract the URL string from the URL object
    response.raise_for_status()  # Raise an exception if the request fails
    logging.info(f'Response status code: {response.status_code}')
    xml_data = response.text
    return xml_data

xml_ids = get_data(url)

# Load the XML file
tree = ET.parse(xml_ids)
root = tree.getroot()

# Access specific elements and attributes
for dataflow in root.iter('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dataflow'):
    dataflow_id = dataflow.get('id')
    urn = dataflow.get('urn')
    # print(f"Dataflow ID: {dataflow_id}")
    # print(f"URN: {urn}")

    # Access child elements
    # for name in dataflow.iter('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name'):
    #     lang = name.get('{http://www.w3.org/XML/1998/namespace}lang')
    #     value = name.text
    #     print(f"Name ({lang}): {value}")

    # for description in dataflow.iter('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Description'):
    #     lang = description.get('{http://www.w3.org/XML/1998/namespace}lang')
    #     value = description.text
    #     print(f"Description ({lang}): {value}")