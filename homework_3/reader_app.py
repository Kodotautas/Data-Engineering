import os
import pandas as pd
import sqlite3
import numpy as np

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# read csv files from ml-latest-small folder (data about movies)
# extract movies year, genres and normalize genres column
# uploads all data to database tables

#get working directory
cwd = os.getcwd()

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
#edge cases like movie: Death Note: Desu nôto (2006–2007). Possible add more in future.
movies['year'] = np.where(movies['title']=='Death Note: Desu nôto (2006–2007)', 2006, movies['year'])
#drop movies without year
movies = movies.dropna()
#delimit data 
movies['genres'] = movies['genres'].str.split('|')
#convert genres to normal form
lens = list(map(len, movies['genres'].values))

#normalize table 
movies = pd.DataFrame({'movieId': np.repeat(movies['movieId'], lens),
                    'title': np.repeat(movies['title'], lens),
                    'year': np.repeat(movies['year'], lens),
                    'genres': np.concatenate(movies['genres'].values)})
 
#RATINGS file
ratings = pd.read_csv(f'{cwd}/ml-latest-small/ratings.csv')
#convert timestamp to datetime and drop timestamp column
ratings['datetime'] = pd.to_datetime(ratings['timestamp'], unit='s')

#TAGS file
tags = pd.read_csv(f'{cwd}/ml-latest-small/tags.csv')
#convert timestamp to datetime and drop timestamp column
tags['datetime'] = pd.to_datetime(tags['timestamp'], unit='s')

print('CSV files parsed')

# ------------------------ CREATE DATABASE AND TABLES ------------------------ #
connection = sqlite3.connect('movies.db')
cursor = connection.cursor()

sql_file = open("create_tables.sql")
sql_as_string = sql_file.read()
cursor.executescript(sql_as_string)
print('Create tables if not exist')


# ---------------------------- LOAD DATA TO TABLES --------------------------- #
links.to_sql('links', con=connection, if_exists = 'replace')
movies.to_sql('movies', con=connection, if_exists = 'replace')
ratings.to_sql('ratings', con=connection, if_exists = 'replace')
tags.to_sql('tags', con=connection, if_exists = 'replace')

connection.commit()
connection.close()
print('Data transfered & connection closed')