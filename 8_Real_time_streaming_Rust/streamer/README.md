#### Rust for Real-Time Data: A GCP Pub/Sub to BigQuery Pipeline
This project delves into the construction of a real-time data pipeline using the strengths of Rust programming in Google Cloud Platform (GCP). The pipeline utilizes Pub/Sub. The ingested data loaded into BigQuery, GCP's serverless data warehouse, for further analysis and exploration.

#### Seismic Data Pipeline
* Processes near real-time seismic events.
* Uses JSON messages via the path: Websocket > Pub/Sub > BigQuery.
* Cloud Scheduler triggers Cloud Run every 14 minutes.
* Event processing takes up to 5 seconds from Websocket API to BigQuery

#### Deployment:
Use Cloud Build: `gcloud builds submit .`

#### Architecture
<div align="center">
  <img src="./diagram/Real-time streaming with rust.jpeg" alt="Data Pipeline Diagram" width="300">
</div>

If you have any questions - please contact me!