use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::time::Instant;
use std::fs::read;
use reqwest::Body;
use std::fs::File;
use std::io::Write;
use zip::write::FileOptions;
use zip::CompressionMethod::Stored;
use zip::ZipWriter;

async fn upload(config: ClientConfig) -> Result<(), Box<dyn std::error::Error>> {

    // Create client.
    let _config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    // Read the file into a byte array
    let file_path = "/home/vytautas/Desktop/archive.zip";
    let data = match read(file_path) {
        Ok(data) => data,
        Err(e) => {
            println!("Failed to read file: {}", e);
            return Err(Box::new(e));
        }
    };

    // Convert Vec<u8> to Body
    let body = Body::from(data);

    // Upload the file
    let upload_type = UploadType::Simple(Media::new(file_path));
    let uploaded = client.upload_object(&UploadObjectRequest {
        bucket: "files-to-experiment".to_string(),
        ..Default::default()
    }, body, &upload_type).await;

    match uploaded {
        Ok(_) => println!("Connection successful, file uploaded."),
        Err(e) => println!("Failed to upload file: {}", e),
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let config = ClientConfig::default().with_auth().await.unwrap();
    upload(config).await?;

    let duration = start.elapsed();
    println!("Upload completed in {} seconds", duration.as_secs_f64());

    Ok(())
}