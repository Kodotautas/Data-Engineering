use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::time::Instant;
use std::fs::read;
use reqwest::Body;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use std::io::Write;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Cursor;

async fn zip_file(file_path: String) -> io::Result<String> {
    let mut buffer = [0; 1024];
    let mut file = File::open(&file_path).await?;
    let zip_file_path = format!("{}.gz", &file_path);
    let mut zip_file = File::create(&zip_file_path).await?;
    let mut encoder = GzEncoder::new(Cursor::new(Vec::new()), Compression::default());

    while let Ok(n) = file.read(&mut buffer).await {
        if n == 0 {
            break;
        }
        encoder.write_all(&buffer[..n])?;
    }

    let inner = encoder.finish()?;
    zip_file.write_all(inner.get_ref().as_slice()).await?;

    Ok(zip_file_path)
}


async fn upload(config: ClientConfig, file_path: String) -> Result<(), Box<dyn std::error::Error>> {

    // Create client.
    let _config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    // Read the file into a byte array
    let data = match read(&file_path) {
        Ok(data) => data,
        Err(e) => {
            println!("Failed to read file: {}", e);
            return Err(Box::new(e));
        }
    };

    // Convert Vec<u8> to Body
    let body = Body::from(data);

    // Upload the file
    let upload_type = UploadType::Simple(Media::new("archive.zip"));
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
    let zip_file_path = zip_file(file_path).await?;

    let duration_zip = start_zip.elapsed();
    println!("File zipped in {} minutes", duration_zip.as_secs_f64() / 60.0);

    let start_upload = Instant::now();

    let config = ClientConfig::default().with_auth().await.unwrap();
    upload(config, zip_file_path).await?;

    let duration_upload = start_upload.elapsed();
    println!("Upload completed in {} minutes", duration_upload.as_secs_f64() / 60.0);

    Ok(())
}