import glob
import argparse
import os
import pandas as pd
import sqlite3
import re
import numpy as np

# ----------------------------- SHORT DESCRIPTION ---------------------------- #



# 1.Take ml-latest-small.zip dataset.
# 2.Design OLTP system (MySQL or SQLite) for it
# 	1.Consider Normal Forms and relationships between tables
# 	2.Save all DDL instructions to a separate .sql file
# 3.Develop an application#1 which 
# 	1.Reads proper CSV files from the dataset
# 	2.Populates OLTP tables with those
# 4.Develop an application#2 which
# 	1.Receives either new ratings or new movies (e.g., via CLI dialogue)
# 	2.Populates proper OLTP tables with it
# 	3.Consider edge cases!
# 5.Provide README.md explaining what your app does and what to use it
# 6.Commit the code to your Coherent Bitbucket.
# 7.Extra: try to avoid using high-level frameworks like Spark and Pandas

#get working directory
cwd = os.getcwd()

# --------------------------------- FUNCTIONS -------------------------------- #

# ------------------------ READ, TRANSFORM DATAFRAMES ------------------------ #
# Read CSV files
#LINKS file
links = pd.read_csv(f'{cwd}/ml-latest-small/links.csv')

#MOVIES file
movies = pd.read_csv(f'{cwd}/ml-latest-small/movies.csv')
#movies:get year, clean
movies['year'] = movies['title'].str.extract(r'(\(\d{4}\))')
movies['year'] = movies['year'].str[-5:].str[:-1]
movies['year'] = movies['year'].apply(lambda x: pd.to_numeric(x, errors = 'coerce')).dropna().astype(int)
movies = movies.drop(columns='Unnamed: 3')
#edge cases like movie: Death Note: Desu nôto (2006–2007)
movies['year'] = np.where(movies['title']=='Death Note: Desu nôto (2006–2007)', 2006, movies['year'])
#drop movies without year
movies = movies.dropna()


#RATINGS file
ratings = pd.read_csv(f'{cwd}/ml-latest-small/ratings.csv')
#convert timestamp to datetime and drop timestamp column
ratings['datetime'] = pd.to_datetime(ratings['timestamp'], unit='s')

#TAGS file
tags = pd.read_csv(f'{cwd}/ml-latest-small/tags.csv')
#convert timestamp to datetime and drop timestamp column
tags['datetime'] = pd.to_datetime(tags['timestamp'], unit='s')


# ------------------------ CREATE DATABASE AND TABLES ------------------------ #
connection = sqlite3.connect('movies.db')
cursor = connection.cursor()
#create links table
cursor.execute('''CREATE TABLE IF NOT EXISTS links
              (movieID INT, imdbId INT, tmdbId INT)''')

#create movies table
cursor.execute('''CREATE TABLE IF NOT EXISTS movies
              (movieId INT, title TEXT, year INT)''')

#create movies table
cursor.execute('''CREATE TABLE IF NOT EXISTS ratings
              (userId INT, movieId INT, rating INT, timestamp INT, datetime TEXT)''')

#create tags table
cursor.execute('''CREATE TABLE IF NOT EXISTS tags
              (userId INT, movieId INT, tag TEXT, timestamp INT, datetime TEXT)''')


# ---------------------------- LOAD DATA TO TABLES --------------------------- #
links.to_sql('links', con=connection, if_exists = 'replace')
movies.to_sql('movies', con=connection, if_exists = 'replace')
ratings.to_sql('ratings', con=connection, if_exists = 'replace')
tags.to_sql('tags', con=connection, if_exists = 'replace')
