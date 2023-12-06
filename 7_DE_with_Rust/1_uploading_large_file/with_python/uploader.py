import os
import time
from google.cloud import storage

class File:
    # def zip_file(source_file_name, destination_file_name):
    #     """Zips a file."""
    #     print(f"Zipping {source_file_name} to {destination_file_name}...")
    #     os.system(f"zip {destination_file_name} {source_file_name}")

    def zip_folder(source_folder, destination_file_name):
        """Zips all files in a folder."""
        print(f"Zipping all files in {source_folder} to {destination_file_name}...")
        os.system(f"zip -r {destination_file_name} {source_folder}/*")

    def upload_file(project_name, bucket_name, destination_file_name, destination_blob_name):
        """Uploads a file to the Google Cloud Storage bucket."""
        storage_client = storage.Client(project=project_name)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        print(f"Uploading {destination_file_name} to {destination_blob_name}...")
        blob.upload_from_filename(destination_file_name)

        print(f"File {destination_file_name} uploaded to {destination_blob_name}.")

# Set variables
project_name = "data-engineering-with-rust"
bucket_name = "files-to-experiment"
source_folder = "/home/vytautas/Desktop/us_elections"
destination_file_name = "/home/vytautas/Desktop/archive.zip"
destination_blob_name = "archive.zip"

start = time.time()
File.zip_folder(source_folder, destination_file_name)
end = time.time()
print(f"Time elapsed: {(end - start) / 60} minutes to zip {source_folder} to {destination_file_name} which size is {os.path.getsize(destination_file_name)} bytes.")

start = time.time()
File.upload_file(project_name, bucket_name, destination_file_name, destination_blob_name)
end = time.time()
print(f"Time elapsed: {(end - start) / 60} minutes to upload {destination_file_name} to {destination_blob_name} which size is {os.path.getsize(destination_file_name)} bytes.")