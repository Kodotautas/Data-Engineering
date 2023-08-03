# This project is about creating Lithuania market statistics ETP pipeline with GCP Dataflow.

# About pipeline:
- parse all valid ids for data from Lithuania Statistics department
- filter out which are not updatable any more and leave later than 2022-01-01
- from Lithuania Statistics via API download all data
- stores .xml files in GCS Buckets by data group for futher use cases

# API source / info:
https://osp.stat.gov.lt/web/guest/rdb-rest

# Don't forget install apache beam GCP:
`pip install apache_beam[gcp]`

# CLI to create Dataflow template:
`python3 main.py -template_location gs://my-bucket/templates/my_template --setup_file ./setup.py --region europe-west1 --output gs://lithuania_statistics/output --runner DataflowRunner --project vl-data-learn --temp_location  gs://lithuania_statistics/temp/`

--Next steps:
create architecture to run full pipeline, before loop through id's