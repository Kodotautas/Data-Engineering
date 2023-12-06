use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::time::Instant;
use tokio::fs::read; 
use reqwest::Body;
use async_zip::ZipEntryBuilder;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::Compression;
use std::path::Path;

async fn zip_file_with_async_zip(file_path: &str) -> std::io::Result<String> {
    let zip_file_path = "archive.zip";
    let mut file = tokio::fs::File::create(zip_file_path).await?;
    let mut writer = ZipFileWriter::with_tokio(&mut file);

    let data = match read(file_path).await {
        Ok(data) => data,
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    };

    let filename = Path::new(file_path).file_name().unwrap().to_str().unwrap();
    let builder = ZipEntryBuilder::new(filename.into(), Compression::Deflate);

    writer.write_entry_whole(builder, &data).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    writer.close().await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

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
    let zip_file_path = zip_file_with_async_zip(&file_path).await?;

    let duration_zip = start_zip.elapsed();
    println!("File zipped in {} minutes", duration_zip.as_secs_f64() / 60.0);

    let start_upload = Instant::now();

    let config = ClientConfig::default().with_auth().await.unwrap();
    upload(config, zip_file_path).await?;

    let duration_upload = start_upload.elapsed();
    println!("Upload completed in {} minutes", duration_upload.as_secs_f64() / 60.0);

    Ok(())
}