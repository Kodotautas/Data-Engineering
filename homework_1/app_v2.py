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
print(cwd)

# --------------------------- READ & SAVE DATAFRAME -------------------------- #
#create dataframe from file
df = pd.read_csv(f"{cwd}/ml-latest-small/movies.csv", sep=',')


# ---------------------------------- FILTER ---------------------------------- #
#get year, clean
# (TO DO: clean full or use regex \(\d{4}\))
df['year'] = df['title'].str[-5:].str[:-1]
df['year'] = df['year'].apply(lambda x: pd.to_numeric(x, errors = 'coerce')).dropna().astype(int)

#find earliest and newest movie by year
min_year = int(df['year'].min())
max_year = int(df['year'].max())

tank_to_fish = {
    "tank_a": "shark, tuna, herring",
    "tank_b": "cod, flounder",
}

parser = argparse.ArgumentParser(description="List fish in aquarium.")
parser.add_argument("tank", type=str)
args = parser.parse_args()

fish = tank_to_fish.get(args.tank, "")
print(fish)