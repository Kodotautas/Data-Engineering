import logging
import io
import zipfile
import urllib.request
from google.cloud import storage


# --------------------------- DOWNLOAD & SAVE DATA --------------------------- #
class DownloadData:
    def download_and_store_zip_file(zip_file_url, bucket_name, folder_name):
        """Downloads a .zip file from the given URL, unzips its contents, and stores them in a GCS bucket.
        Args:
            zip_file_url: The URL of the .zip file to download.
            bucket_name: The name of the GCS bucket to store the files in.
            folder_name: The name of the folder within the bucket to store the files.
        """

        # Initialize the GCS client.
        storage_client = storage.Client()

        # Download the .zip file to a temporary file.
        with urllib.request.urlopen(zip_file_url) as response:
            with io.BytesIO(response.read()) as temp_file:
                logging.info(f'Downloaded {zip_file_url}')
                
                # Unzip the .zip file.
                with zipfile.ZipFile(temp_file) as zip_file:
                    for file_name in zip_file.namelist():
                        # Read the file contents from the .zip file.
                        file_contents = zip_file.read(file_name)
                        
                        # Upload the file contents to the GCS bucket.
                        bucket = storage_client.bucket(bucket_name)
                        blob = bucket.blob(f'{folder_name}/{file_name}')
                        blob.upload_from_string(file_contents)
                        
                        logging.info(f'Uploaded {file_name} to {bucket_name}/{folder_name}')


# ----------------------------------- MAIN ----------------------------------- #
if __name__ == "__main__":
    zip_file_url = "https://www.regitra.lt/atvduom/Atviri_JTP_parko_duomenys.zip"
    bucket_name = "lithuania_statistics"
    folder_name = "companies_cars"

    DownloadData.download_and_store_zip_file(zip_file_url, bucket_name, folder_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()