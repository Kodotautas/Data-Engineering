#### Rust for Real-Time Data: A GCP Pub/Sub to BigQuery Pipeline
This project delves into the construction of a real-time data pipeline using Rust programming in Google Cloud Platform (GCP). The pipeline utilizes Pub/Sub. The ingested data loaded into BigQuery, GCP's serverless data warehouse for further analysis and exploration.

To ensure continuous streaming, set up Cloud Scheduler with jobs running every less < 15 minutes. This prevents Cloud Run from automatically scaling down after inactivity.

Please note while Cloud Run is a powerful platform for many scenarios, it's not ideal for very long real-time streaming applications. For these use cases, consider using a Google Kubernetes Engine (GKE) cluster, which offers greater control and flexibility for managing long-running processes.

#### Seismic Data Pipeline
* Processes near real-time seismic events.
* Uses JSON messages via the path: Websocket > Pub/Sub > BigQuery.
* Cloud Scheduler triggers Cloud Run every 14 minutes.
* Event processing takes up to 5 seconds from Websocket API to BigQuery

Deploy run: `gcloud builds submit .`

#### Architecture
<div align="center">
  <img src="./diagram/Real-time streaming with rust.jpeg" alt="Data Pipeline Diagram" width="300">
</div>

##### Main stats from Cloud Run during streaming:
<div align="center">
  <img src="./diagram/Billable container instance time.jpg" alt="Billable container instance time" width="300">
</div>

<div align="center">
  <img src="./diagram/Received bytes.jpg" alt="Received bytes" width="300">
</div>

<div align="center">
  <img src="./diagram/Request count.jpg" alt="Request count" width="300">
</div>

<div align="center">
  <img src="./diagram/Container CPU utilisation.jpg" alt="CPU utilization" width="300">
</div>

<div align="center">
  <img src="./diagram/Max. concurrent requests.jpg" alt="Concurrent requests" width="300">
</div>

Please don’t hesitate to contact me for any clarifications :loudspeaker: