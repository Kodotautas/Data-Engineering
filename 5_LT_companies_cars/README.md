# This project is about to create dashboard about Lithunia companies owned cars using GCP (Dataflow, Bigquery, Looker)

# About pipeline:
- get information about companies cars from Regitra
- get information about companies and it's names
- store data in BigQuery
- analyze companies and create dashboard with Looker
- set schedule to pipeline and dashboard


### Pipeline diagram:
<img src="./dataflow_pipeline/diagram/lt_statistics_pipeline.jpeg" alt="Data Pipeline Diagram" width="300">

- to do:
- optimize code as best as can
- create pipeline with Dataflow
- create looker studio dashboard
- need to create dbt model or schedule bigquery table, how?
- possible connect more data like companies employes number?
- is there relationship between luxury cars and salary
