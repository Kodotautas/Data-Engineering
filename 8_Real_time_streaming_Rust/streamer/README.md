#### Overview
Data engineering project with Rust to get real time streaming in GCP using Pub/Sub and load data to Bigquery.

It load seismic (almost) real time events. A Json message is sent through Websocket when an event is inserted or updated.

Cloud build: `gcloud builds submit .`

#### Key Features
- `Real time streaming`: application allows real time stream events to BigQuery for later use for analytics / reporting.
- `Performance` from Websocket to Bigquery table it took < 1s total time of processing.

#### Architecture
<div align="center">
  <img src="./diagram/vno_app_architecture.jpeg" alt="Data Pipeline Diagram" width="300">
</div>

#### Access the Web Application
[Here](https://vno-viewpoint.appspot.com/) 

For inquiries or feedback, feel free to reach out.