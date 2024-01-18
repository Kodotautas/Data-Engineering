use google_cloud_storage::client::{ClientConfig, Client};
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
struct Downloader;


impl Downloader {
    async fn run(_config: ClientConfig, url: &str, bucket: &str, object_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Create client.
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(config);

        // Download the file
        let response = reqwest::get(url).await?;
        let bytes = response.bytes().await?;

        // Convert Bytes to Vec<u8>
        let bytes_vec = bytes.to_vec();

        // Upload the file
        let upload_type = UploadType::Simple(Media::new(object_name.to_string()));
        let _uploaded = client.upload_object(&UploadObjectRequest {
            bucket: bucket.to_string(),
            ..Default::default()
        }, bytes_vec, &upload_type).await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "/home/vytautas/.config/gcloud/application_default_credentials.json");
    
    let config = ClientConfig::default();
    let url = "https://get.data.gov.lt/datasets/gov/ird/anr/KetPazeidejas/:format/csv";
    let bucket = "lithuania_statistics";
    let object_name = "my-object";

    Downloader::run(config, url, bucket, object_name).await?;

    Ok(())
}