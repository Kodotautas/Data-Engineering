import argparse
import requests, zipfile, io
import os
import pandas as pd
import re

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# read csv file about movies (from grouplens)
# extract movies release year & main genre
# by user inputs full dataframe is filtered by year and/or main genre
# results are printed to console and exported as parquet format file 

#get working directory
cwd = os.getcwd()

# ---------------------------------- PARSER ---------------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument("--year", default=2018, action="store_true", help="Filter dataframe by year")
parser.add_argument("--genre", default='Comedy', action="store_true", help="Filter dataframe by genre")
args = parser.parse_args()

# -------------------------- READ, MODIFY DATAFRAME -------------------------- #
#parse dataframe from csv
# df = pd.read_csv(f"{cwd}/ml-latest-small/movies.csv", sep=',')
df = pd.read_csv(f"{cwd}/ml-latest-small/movies.csv",  sep=',')

#get year, clean
# (TO DO: clean full or use regex \(\d{4}\))
df['year'] = df['title'].str[-5:].str[:-1]
df['year'] = df['year'].apply(lambda x: pd.to_numeric(x, errors = 'coerce')).dropna().astype(int)

#find earliest and newest movie by year
min_year = int(df['year'].min())
max_year = int(df['year'].max())

# ---------------------------------- FILTERS --------------------------------- #
if args.year:
    print(df[df['year'] == 2017])