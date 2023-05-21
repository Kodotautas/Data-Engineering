import os
import pandas as pd
import xml.etree.ElementTree as ET

xml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docs'))


xml_data = '''
<mes:Structure xsi:schemaLocation="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message https://osp-rs.stat.gov.lt/xsd_scheme/SDMXMessage.xsd http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common https://osp-rs.stat.gov.lt/xsd_scheme/SDMXCommon.xsd http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic https://osp-rs.stat.gov.lt/xsd_scheme/SDMXDataGeneric.xsd">
<mes:Header>
<mes:ID>FDFE2235679B4AEFB4C456A7DD57D1DF</mes:ID>
<mes:Test>true</mes:Test>
<mes:Prepared>2023-05-21T21:37:42.397000000+03:00</mes:Prepared>
<mes:Sender id="unknown"/>
<mes:Receiver id="not_supplied"/>
</mes:Header>
<mes:Structures>
<str:Dataflows>
<str:Dataflow id="S1R003_M8020420" urn="urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=LSD:S1R003_M8020420(1.0)" isExternalReference="false" agencyID="LSD" isFinal="false" version="1.0">
<com:Name xml:lang="lt">
S1R003 - Būstai, aprūpinti karštu vandeniu centralizuotai iš miesto (rajono) šilumos tinklų
</com:Name>
<com:Name xml:lang="en">
S1R003 - Dwellings with centralised hot water supply from city/town (district) heating networks
</com:Name>
<com:Description xml:lang="lt">
Gyvenamoji vietovė (2009 - 2009) (Atnaujinta: 2013-01-16)
</com:Description>
<com:Description xml:lang="en">
Place of residence (2009 - 2009) (Was renewed: 2013-01-16)
</com:Description>
<str:Structure>
</str:Structure>
</str:Dataflow>
</str:Dataflows>
</mes:Structures>
</mes:Structure>
'''


# Parse the xml file
root = ET.fromstring(xml_data)

# Find the relevant elements
dataflows = root.findall('.//{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dataflow')

# Extract the id attribute and com:Name values
for dataflow in dataflows:
    dataflow_id = dataflow.get('id')
    com_names = dataflow.findall('.//{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name[@xml:lang="en"]')

    for name in com_names:
        name_value = name.text
        print(f"Dataflow id: {dataflow_id}, com:Name (en): {name_value}")