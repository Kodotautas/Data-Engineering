import pandas as pd
import xml.etree.ElementTree as ET
import requests
from lxml import etree

url = 'https://osp-rs.stat.gov.lt/rest_xml/data/S3R168_M3010101_1/?startPeriod=2005-01&endPeriod=2005-03'

response = requests.get(url)
print(f"Response status code: {response.status_code}")

xml_data = response.text

# Parse the XML file
tree = etree.parse('/home/vytautas/Documents/GitHub/Data-Engineering/4_Lithuania_statistics/data/test_data.xml')
root = tree.getroot()

# Extract the data
data = []
for obs in root.findall('.//g:Obs', namespaces=root.nsmap):
    row = {}
    for value in obs.findall('.//g:Value', namespaces=root.nsmap):
        row[value.get('id')] = value.get('value')
    row['value'] = obs.find('.//g:ObsValue', namespaces=root.nsmap).get('value')
    data.append(row)

# Create a DataFrame
df = pd.DataFrame(data)
print(df)