# script downloads file from url

import requests

url = "https://atvira.sodra.lt/imones/downloads/2023/monthly-2023.csv.zip"

def download_file(url):
    local_filename = url.split('/')[-1]
    with requests.get(url) as r:
        with open(local_filename, 'wb') as f:
            f.write(r.content)
    return local_filename

download_file(url)