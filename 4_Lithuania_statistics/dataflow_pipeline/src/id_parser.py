import os
import argparse
import xml.etree.ElementTree as ET
import pandas as pd
import requests

cwd = os.getcwd()

# -------------------------------- ID's parser ------------------------------- #
class IdParser:
    def download_data(url):
        """A function that downloads all products ID's from a LT Statistics URL.
        and saves it to a file.
        Args:
            url (str): The URL to download the data from.
        Returns:
            str: The downloaded data as a xml string.
        """
        response = requests.get(url)
        print(f'Downloaded {len(response.content)} bytes.')
        return response.content

    def parse_xml_to_dataframe(xml_data):
        """Parses an XML string to a Pandas DataFrame.
        Args:
            xml_data (str): The XML string to parse.
        Returns:
            pd.DataFrame: The parsed XML data as a Pandas DataFrame.
        """

        # Define the namespace mapping
        namespaces = {
            "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
            "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
        }

        root = ET.fromstring(xml_data)
        rows = []
        for dataflow in root.findall(".//str:Dataflow", namespaces):
            row = {
                "id": dataflow.attrib["id"],
                "urn": dataflow.attrib["urn"],
                "agencyID": dataflow.attrib["agencyID"],
                "isFinal": dataflow.attrib["isFinal"],
                "version": dataflow.attrib["version"],
                "name": dataflow.find(".//com:Name", namespaces).text,
                "description": dataflow.find(".//com:Description", namespaces).text,
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        print(f'Parsed {len(df)} rows.')
        return df

    def df_transform(df, months=12):
        """Transforms the parsed XML data.
        Args:
            df (pd.DataFrame): The parsed XML data as a Pandas DataFrame.
        Returns:
            pd.DataFrame: The transformed data as a Pandas DataFrame.
        """

        # fiter out column 'description' which has value 'Neatnaujinamas' use regex
        df = df[~df['description'].str.contains('Neatnaujinamas', regex=True)]
        
        # replace /, \, *, ?, ", <, >, |, : with _
        df['description'] = df['description'].str.replace(r'[/\\*?"<>|:]', '_')

        # extract date from column 'description' and create new column 'update_date' and drop null values
        df['update_date'] = df['description'].str.extract('Atnaujinta:\s+(\d{4}-\d{2}-\d{2})')
        df = df.dropna(subset=['update_date'])
        
        # filter rows by date later than previous month
        df['update_date'] = pd.to_datetime(df['update_date'])
        df = df[df['update_date'] > pd.to_datetime('today') - pd.offsets.MonthBegin(months)]

        # drop colums
        df = df.drop(columns=['agencyID', 'isFinal', 'version', 'urn'])
        return df
    
    @classmethod
    def parse_args(cls):
        parser = argparse.ArgumentParser(description='ID Parser')
        parser.add_argument('--months', type=int, default=12, help='Number of months to look back.')
        args = parser.parse_args()
        cls.months = args.months

if __name__ == "__main__":
    IdParser.parse_args()
    xml_data = IdParser.download_data('https://osp-rs.stat.gov.lt/rest_xml/dataflow/')
    df = IdParser.parse_xml_to_dataframe(xml_data)
    df = IdParser.df_transform(df, months=IdParser.months)
    df.to_csv(f'{cwd}/4_Lithuania_statistics/dataflow_pipeline/data/parsed_all_ids.csv', index=False)
    print(f'Parsed {len(df)} rows.')