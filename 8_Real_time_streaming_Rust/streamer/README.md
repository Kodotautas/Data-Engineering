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

#### Stats from GCP Cloud Run
<div align="center">
  <img src="./diagram/Billable container instance time.png" alt="Billable container instance time" width="300">
</div>

<div align="center">
  <img src="./diagram/Container CPU utilisation.png" alt="CPU utilization" width="300">
</div>

<div align="center">
  <img src="./diagram/Container memory utilisation.png" alt="Memory utilization" width="300">
</div>

<div align="center">
  <img src="./diagram/Max. concurrent requests.png" alt="Concurrent requests" width="300">
</div>

<div align="center">
  <img src="./diagram/Received bytes.png" alt="Received bytes" width="300">
</div>

<div align="center">
  <img src="./diagram/Request count.png" alt="Request count" width="300">
</div>

Please don’t hesitate to contact me for any clarifications :loudspeaker: