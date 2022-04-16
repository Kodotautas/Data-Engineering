import glob
import argparse
import os
from random import randint
import pandas as pd

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# read csv files from ml-latest-small folder (data about movies)
# uploads data to database
# get a view of top-3 actively voting users per each genre for a given year
# results are printed to console and exported as parquet format file 

#get working directory
cwd = os.getcwd()
print(cwd)

# -------------------------- READ, MERGE DATAFRAMES -------------------------- #
# indentify all csv files
csv_files = glob.glob(f'{cwd}/ml-latest-small' + '/*.csv')

#get csv file names. movies.csv to movies
fns = [os.path.splitext(os.path.basename(x))[0] for x in csv_files]

#read to separate dataframes
d = {}
for i in range(len(fns)):
    d[fns[i]] = pd.read_csv(csv_files[i])

    
# SEARCH: create multiple dataframes from dictionary
