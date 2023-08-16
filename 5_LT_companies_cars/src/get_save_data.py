import logging
import io
import zipfile
import urllib.request
from google.cloud import storage
from datetime import date

# Configuration
current_year = date.today().year
BUCKET_NAME = "lithuania_statistics"
FOLDER_NAME = "companies_cars"
ZIP_URL_SODRA = f"https://atvira.sodra.lt/imones/downloads/{current_year}/monthly-{current_year}.csv.zip"
ZIP_URL_REGITRA = "https://www.regitra.lt/atvduom/Atviri_JTP_parko_duomenys.zip"


# ------------------------------- GCS UPLOADER ------------------------------- #
class GCSUploader:
    @staticmethod
    def upload_file_to_bucket(file_contents, file_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{FOLDER_NAME}/{file_name}')
        blob.upload_from_string(file_contents)
        logging.info(f'Uploaded {file_name} to {BUCKET_NAME}/{FOLDER_NAME}')


# -------------------------------- DOWNLOADER -------------------------------- #
class Downloader:
    @staticmethod
    def download_zip_file(zip_file_url):
        try:
            headers = {'User-Agent': 'Your-User-Agent-String'}
            request = urllib.request.Request(zip_file_url, headers=headers)
            with urllib.request.urlopen(request) as response:
                return response.read()
        except urllib.error.HTTPError as http_error:
            logging.error(f"HTTP Error {http_error.code}: {http_error.reason}")
            return None
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None

    @staticmethod
    def extract_zip_contents(zip_file_bytes):
        with io.BytesIO(zip_file_bytes) as temp_file:
            with zipfile.ZipFile(temp_file) as zip_file:
                extracted_files = {}
                for file_name in zip_file.namelist():
                    file_contents = zip_file.read(file_name)
                    extracted_files[file_name] = file_contents
                return extracted_files
    
class FinalUploader:
    def main(zip_file_url):
        zip_file_bytes = Downloader.download_zip_file(zip_file_url)
        if zip_file_bytes:
            extracted_files = Downloader.extract_zip_contents(zip_file_bytes)
            for file_name, file_contents in extracted_files.items():
                GCSUploader.upload_file_to_bucket(file_contents, file_name)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    FinalUploader.main(ZIP_URL_SODRA)
    FinalUploader.main(ZIP_URL_REGITRA)