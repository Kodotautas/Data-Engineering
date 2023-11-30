use google_cloud_storage::Client;
use std::path::Path;

fn main() {
    // Set the project ID and bucket name
    let project_id = "data-engineering-with-rust";
    let bucket_name = "files-to-experiment";

    // Replace with the path to the file you want to upload
    let file_path = "/home/vytautas/Desktop/DSCF1818,jpg";

    // Create a new GCS client
    let client = Client::new(project_id).expect("Failed to create GCS client");

    // Get the file name from the file path
    let file_name = Path::new(file_path)
        .file_name()
        .expect("Invalid file path")
        .to_str()
        .expect("Invalid file name");

    // Upload the file to the GCS bucket
    client
        .bucket(bucket_name)
        .upload(file_path, file_name)
        .expect("Failed to upload file");

    println!("File uploaded successfully!");
}
