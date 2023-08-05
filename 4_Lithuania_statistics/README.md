# This project is about creating Lithuania market statistics pipeline with GCP Dataflow.

# About pipeline:
- parse all valid ids for data from Lithuania Statistics department
- filter out which are not updatable any more and leave later than 2022-01-01
- from Lithuania Statistics via API download all data
- stores .xml files in GCS Buckets by data group for futher use cases
- final Dataflow pipeline updates once per month only newly updated LT statistics

# API source / info:
https://osp.stat.gov.lt/web/guest/rdb-rest

## Don't forget install apache beam GCP:
`pip install apache_beam[gcp]`

# Dataflow CLI:
`python3 main.py --setup_file ./setup.py --region europe-west1 --output gs://lithuania_statistics/output --runner DataflowRunner --project vl-data-learn --staging_location gs://vl-data-learn/dataflow/staging --temp_location  gs://lithuania_statistics/temp/ --template_location gs://lithuania_statistics/templates/lt-statistics-template`

- run pipeline remove last part: template_location


--Next steps:
- add months param to cli
- save template with month 1
- create and schedule pipeline