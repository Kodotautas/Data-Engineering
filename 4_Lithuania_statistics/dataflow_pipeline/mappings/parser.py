import pandas as pd
import xml.etree.ElementTree as ET
import os

# ----------------------------------- PARSE ---------------------------------- #
cwd = os.getcwd()
xml_string = open(f'{cwd}/4_Lithuania_statistics/dataflow_pipeline/mappings/all_products_ids.xml', 'r').read()

root = ET.fromstring(xml_string)

# Create a Pandas DataFrame
df = pd.DataFrame()

# Iterate over the `Dataflow` elements
for dataflow in root.findall(".//str:Dataflow"):
    row = {}
    row["id"] = dataflow.attrib["id"]
    row["urn"] = dataflow.attrib["urn"]
    row["agencyID"] = dataflow.attrib["agencyID"]
    row["isFinal"] = dataflow.attrib["isFinal"]
    row["version"] = dataflow.attrib["version"]

    # Iterate over the `Name` elements
    for name in dataflow.findall(".//com:Name"):
        row[name.attrib["xml:lang"]] = name.text

    df = df.append(row, ignore_index=True)


# ------------------------------ TRANSFORMATIONS ----------------------------- #
# Print the DataFrame
print(df)
