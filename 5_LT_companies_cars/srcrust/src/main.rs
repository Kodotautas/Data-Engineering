use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::time::Instant;
use std::process::Command;

struct Processor;

impl Processor {
    async fn download_and_upload(_config: ClientConfig, url: &str, bucket: &str, object_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Create client.
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(config);

        // Start timing
        let download_start = Instant::now();

        // Download the file
        let response = reqwest::get(url).await?;
        let bytes = response.bytes().await?;

        // Print download time in seconds
        println!("Download data took {} seconds", download_start.elapsed().as_secs());

        // Convert Bytes to Vec<u8>
        let bytes_vec = bytes.to_vec();

        // Start timing upload
        let upload_start = Instant::now();

        // Upload the file
        let upload_type = UploadType::Simple(Media::new(object_name.to_string()));
        let _uploaded = client.upload_object(&UploadObjectRequest {
            bucket: bucket.to_string(),
            ..Default::default()
        }, bytes_vec, &upload_type).await?;

        // Print upload time
        println!("Upload took {} seconds", upload_start.elapsed().as_secs());

        Ok(())
    }

    // Load data to BigQuery
    fn load_csv_to_bigquery(dataset: &str, table: &str, bucket: &str, file: &str) -> std::io::Result<()> {
        let output = Command::new("bq")
            .arg("load")
            .arg("--autodetect")
            .arg("--source_format=CSV")
            .arg(format!("{}.{}", dataset, table))
            .arg(format!("gs://{}/{}", bucket, file))
            .output()?;
    
        if !output.status.success() {
            eprintln!("Error: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        println!("Data loaded to BigQuery");

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "/home/vytautas/.config/gcloud/application_default_credentials.json");
    
    let config = ClientConfig::default();
    let url = "https://get.data.gov.lt/datasets/gov/ird/anr/KetPazeidejas/:format/csv";
    let bucket = "lithuania_statistics";
    let object_name = "lithuania_statistics/KetPazeidejas.csv";

    Processor::download_and_upload(config, url, bucket, object_name).await?;

    Processor::load_csv_to_bigquery("lithuania_statistics", "ket_pazeidejas_raw", "lithuania_statistics", "lithuania_statistics/KetPazeidejas.csv")?;

    Ok(())
}