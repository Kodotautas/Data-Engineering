# This project is about creating Lithuania market statistics ETP pipeline with GCP Dataflow.

# About project:
Created simple data pipeline which:
- from Lithuania Statistics portal via API download population data
- stores it in GCS Bucket
- visualize data in Looker dashboard


# API source / info:
https://osp.stat.gov.lt/web/guest/rdb-rest

# Don't forget install apache beam GCP:
pip install apache_beam[gcp]

# CLI to create Dataflow template:
`python3 main.py -template_location gs://my-bucket/templates/my_template --setup_file /setup.py`

--Next step: build dataflow pipeline
-- name 'requests' is not defined | something wrong in gcp 