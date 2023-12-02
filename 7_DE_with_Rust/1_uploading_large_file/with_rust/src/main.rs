// filename: src/main.rs
// fn main() {
//     println!("Hello, world! Let's use data with Rust!");
// }

use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
// use google_cloud_storage::sign::SignedURLOptions;
// use google_cloud_storage::sign::SignBy;
// use google_cloud_storage::sign::SignedURLMethod;
use google_cloud_storage::http::Error;
// use google_cloud_storage::http::objects::download::Range;
// use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
// use tokio::task::JoinHandle;
// use std::fs::File;
// use std::io::BufReader;
// use std::io::Read;

async fn run(config: ClientConfig) -> Result<(), Error> {

    // Create client.
    let client = Client::new(config);

    // Upload the file
    let upload_type = UploadType::Simple(Media::new("file.png"));
    let uploaded = client.upload_object(&UploadObjectRequest {
        bucket: "bucfiles-to-experiment".to_string(),
        ..Default::default()
    }, "hello world".as_bytes(), &upload_type).await;

    match uploaded {
        Ok(_) => println!("Connection successful, file uploaded."),
        Err(e) => println!("Failed to upload file: {}", e),
    }

    // Download the file
//     let data = client.download_object(&GetObjectRequest {
//         bucket: "bucket".to_string(),
//         object: "file.png".to_string(),
//         ..Default::default()
//    }, &Range::default()).await;

    // Create signed url with the default key and google-access-id of the client
    // let url_for_download = client.signed_url("bucket", "foo.txt", None, None, SignedURLOptions::default());
    // let url_for_upload = client.signed_url("bucket", "foo.txt", None, None, SignedURLOptions {
    //     method: SignedURLMethod::PUT,
    //     ..Default::default()
    // });

    Ok(())
}

fn main() {
    let config = ClientConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run(config)).unwrap();
}