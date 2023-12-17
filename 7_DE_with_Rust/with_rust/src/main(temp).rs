use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gcp_sa_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .unwrap();

    let _client = gcp_bigquery_client::Client::from_service_account_key_file(&gcp_sa_key).await?;

    return Ok(());
}
