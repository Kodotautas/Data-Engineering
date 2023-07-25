import os
import xml.etree.ElementTree as ET
import pandas as pd

cwd = os.getcwd()

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
    return df

if __name__ == "__main__":
    xml_data = open(f'{cwd}/4_Lithuania_statistics/dataflow_pipeline/mappings/all_products_ids.xml', "r").read()
    df = parse_xml_to_dataframe(xml_data)
    df.to_csv(f'{cwd}/4_Lithuania_statistics/dataflow_pipeline/data/parsed_all_ids.csv', index=False)
    print(f'Parsed {len(df)} rows.')