# unzip .zip file
import zipfile

with zipfile.ZipFile("/home/vytautas/Documents/GitHub/Data-Engineering/5_LT_companies_cars/src/monthly-2023.csv.zip", 'r') as zip_ref:
    zip_ref.extractall()
    print("File unzipped")