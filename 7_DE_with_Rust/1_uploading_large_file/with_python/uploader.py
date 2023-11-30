import os
import time
from google.cloud import storage

def upload_file(project_name, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the Google Cloud Storage bucket."""
    storage_client = storage.Client(project=project_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Set variables
project_name = "data-engineering-with-rust"
bucket_name = "files-to-experiment"
source_file_name = "/home/vytautas/Desktop/archive.zip"
destination_blob_name = source_file_name.split("/")[-1]

start = time.time()
upload_file(project_name, bucket_name, source_file_name, destination_blob_name)
end = time.time()
print(f"Time elapsed: {end - start} seconds to upload {source_file_name} to {destination_blob_name} which size is {os.path.getsize(source_file_name)} bytes.")