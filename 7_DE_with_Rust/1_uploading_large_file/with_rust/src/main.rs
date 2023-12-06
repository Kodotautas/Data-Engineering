use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::time::Instant;
use tokio::fs::read; 
use reqwest::Body;
use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;
use zip::write::FileOptions;
use zip::CompressionMethod::Stored;
use std::path::Path;

fn zip_file(file_path: &str) -> std::io::Result<String> {
    let zip_file_path = "/home/vytautas/Desktop/archive.zip";
    let file = File::create(zip_file_path)?;
    let mut zip = zip::ZipWriter::new(file);
    let options = FileOptions::default()
        .compression_method(Stored)
        .unix_permissions(0o755);

    let path = Path::new(file_path);
    let file_name = path.file_name().unwrap().to_str().unwrap();

    zip.start_file(file_name, options)?;

    let mut buffer = [0; 1024 * 1024]; // 1MB buffer
    let mut reader = BufReader::new(File::open(&path)?);

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        zip.write_all(&buffer[..n])?;
    }

    zip.finish()?;

    Ok(zip_file_path.to_string())
}

async fn upload(config: ClientConfig, file_path: String) -> Result<(), Box<dyn std::error::Error>> {

    // Create client.
    let _config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    // Read the file into a byte array
    let data = match read(&file_path).await {
        Ok(data) => data,
        Err(e) => {
            println!("Failed to read file: {}", e);
            return Err(Box::new(e));
        }
    };

    // Convert Vec<u8> to Body
    let body = Body::from(data);

    // Upload the file
    let upload_type = UploadType::Simple(Media::new("application/zip"));
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
    let start_zip = Instant::now();

    let file_path = "/home/vytautas/Desktop/itineraries.csv".to_string();
    let zip_file_path = zip_file(&file_path)?;

    let duration_zip = start_zip.elapsed();
    println!("File zipped in {} minutes", duration_zip.as_secs_f64() / 60.0);

    let start_upload = Instant::now();

    let config = ClientConfig::default().with_auth().await.unwrap();
    upload(config, zip_file_path).await?;

    let duration_upload = start_upload.elapsed();
    println!("Upload completed in {} minutes", duration_upload.as_secs_f64() / 60.0);

    Ok(())
}