import argparse
import os
import pandas as pd
import re
import numpy as np

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# read csv file about movies (from grouplens)
# extract movies release year
# by user inputs full dataframe is filtered by year or genre
# results are printed to console and exported as parquet format file 

#get working directory
cwd = os.getcwd()

# --------------------------------- FUNCTIONS -------------------------------- #
def export_parquet(df):
    """function export parquet file to cwd/parquet directory
    Args:
        df (dataframe): filtered dataframe by user input
    """    
    df.to_parquet(f"{cwd}/files/parquet/movies.parquet.gzip'", compression='brotli')  
    print('Parquet file exported')


# -------------------------- READ, MODIFY DATAFRAME -------------------------- #
#parse dataframe from csv
df = pd.read_csv(f"{cwd}/files/ml-latest-small/movies.csv",  sep=',', encoding="UTF-8")

#get year, clean
df['year'] = df['title'].str.extract(r'(\(\d{4}\))')
df['year'] = df['year'].str[-5:].str[:-1]
df['year'] = df['year'].apply(lambda x: pd.to_numeric(x, errors = 'coerce')).dropna().astype(int)
#edge cases like movie: Death Note: Desu nôto (2006–2007)
df['year'] = np.where(df['title']=='Death Note: Desu nôto (2006–2007)', 2006, df['year'])
#drop movies without year
df = df.dropna()

#extract unique list of genres
list_of_genres = df['genres'].unique()


# ---------------------------------- PARSER ---------------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-Y', '--year', dest='year', type=int, nargs='+', help='Write one or multiple years. e.g. 2014 2017')
parser.add_argument('-G','--genre', dest='genre', type=str, help="Write movie genre or multiple genres. e.g. 'Drama', 'Crime|Drama'")

args = parser.parse_args()


# ---------------------------------- FILTERS --------------------------------- #
# filter, print and export by year(s)
if args.year:
    yearlist = [int(i) for i in args.year]
    filtered = (df[df['year'].isin(yearlist)])
    print(filtered)
    export_parquet(filtered)    

 # filter, print and export by genre    
if args.genre:
    filtered = df[df['genres'] == args.genre]
    print(filtered)
    export_parquet(filtered)