### This project is about to create dashboard about Lithuania companies which own luxury cars, build using GCP (Dataflow, Storage, Bigquery, Looker)

### About pipeline:
- get information about companies cars from Regitra
- get information about companies and it's names
- store data in BigQuery
- analyze companies and create dashboard with Looker
- set schedule to pipeline and dashboard

### Dataflow:
#### Create template:
`python3 main.py --setup_file ./setup.py --region europe-west1 --output gs://lithuania_statistics/output --runner DataflowRunner --project vl-data-learn --staging_location gs://vl-data-learn/dataflow/staging --temp_location  gs://lithuania_statistics/temp/ --template_location gs://lithuania_statistics/templates/lt-cars-dashboard-template`

### Run:
`python3 main.py --setup_file ./setup.py --region europe-west1 --output gs://lithuania_statistics/output --runner DataflowRunner --project vl-data-learn --staging_location gs://vl-data-learn/dataflow/staging --temp_location  gs://lithuania_statistics/temp/`

### Architecture:
<img src="./diagram/lt_cars_dashboard.jpeg" alt="Data Pipeline Diagram" width="300">

##### Dashboard link: [Here](https://lookerstudio.google.com/s/uMGgaIay7bA)

- test dataflow process
- save new pipeline with dataflow to run it month.10 day
- add individuals cars data, question: what dashboard?
- implement model via dataform?