import requests
from bs4 import BeautifulSoup
import re

tractor_url = 'https://data.gov.lt/datasets/278/'

# function to get all urls with .csv files
def get_urls(url):
    # get html code
    r = requests.get(url)
    # parse html code
    soup = BeautifulSoup(r.content, 'html.parser')
    # find all urls
    urls = soup.find_all('a', href=re.compile(r'.csv'))
    # create list with urls
    urls_list = []
    for url in urls:
        urls_list.append('https://data.gov.lt' + url['href'])
    return urls_list