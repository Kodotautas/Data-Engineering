# This project is about to create dashboard about Lithunia companies owned cars using GCP (Dataflow, Bigquery, Looker)

# About pipeline:
- get information about companies cars from Regitra
- get information about companies and it's names
- store data in BigQuery
- analyze companies and create dashboard with Looker
- set schedule to pipeline and dashboard

# Dataflow:
### Create template:
`python3 main.py --setup_file ./setup.py --region europe-west1 --output gs://lithuania_statistics/output --runner DataflowRunner --project vl-data-learn --staging_location gs://vl-data-learn/dataflow/staging --temp_location  gs://lithuania_statistics/temp/ --template_location gs://lithuania_statistics/templates/lt-cars-dashboard-template`

## Run:
`python3 main.py --setup_file ./setup.py --region europe-west1 --output gs://lithuania_statistics/output --runner DataflowRunner --project vl-data-learn --staging_location gs://vl-data-learn/dataflow/staging --temp_location  gs://lithuania_statistics/temp/ --months 1`

### Pipeline diagram:
<img src="./dataflow_pipeline/diagram/lt_statistics_pipeline.jpeg" alt="Data Pipeline Diagram" width="300">

- to do:
- create pipeline with Dataflow
- create looker studio dashboard
- need to create dbt model or schedule bigquery table, how? scheduled queries
- is there relationship between luxury cars and salary
 - other analysis insights
