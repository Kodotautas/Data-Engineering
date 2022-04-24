import glob
import argparse
import os
import pandas as pd
import sqlite3

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# read csv files from ml-latest-small folder (data about movies)
# uploads all data to database
# get a view of top-3 actively voting users per each genre for a given year (arguments via CLI)
# results are exported to csv file 

#get working directory
cwd = os.getcwd()

# --------------------------------- FUNCTIONS -------------------------------- #
def export_to_csv(df):
    """function export csv file of any dataframe
    Args:
        df (dataframe): filtered dataframe by sql query
    """    
    df.to_csv(f"{cwd}/results.csv")  
    print('Results exported to csv')

def run_query_and_export(conn, query, year_cli, genre_cli):
    """run SQL query and results is exported to csv file

    Args:
        conn (str): connection to database
        query (str): SQL query which function will run
        year_cli (int): year input from CLI
        genre_cli (str): genre (will be excluded) input from CLI 
    """
    cur = conn.cursor()

    df = pd.read_sql(query, conn, params={"year":year_cli,"genre":genre_cli})
    export_to_csv(df)
    

# ---------------------------- ARGPARSER & FILTERS --------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-Y', '--year', dest='year', type=int, nargs='+', action='append', help='Write year. e.g. 2014')
parser.add_argument('-G','--genre', dest='genre', type=str, action='append', help="Write movie genre or multiple genres. e.g. 'Drama' or 'Crime|Drama'")

args = parser.parse_args()

#save as variables from year, genre inputs
year_cli = args.year[0][0]
genre_cli = args.genre[0]

parser.print_help()
print(f'Year: {year_cli}')
print(f'Excluded genre: {genre_cli}') 


# ------------------------ READ, TRANSFORM DATAFRAMES ------------------------ #
# Read CSV files
#links file
links = pd.read_csv(f'{cwd}/ml-latest-small/links.csv')

#movies file
movies = pd.read_csv(f'{cwd}/ml-latest-small/movies.csv')
#get year, clean
movies['year'] = movies['title'].str[-5:].str[:-1]
movies['year'] = movies['year'].apply(lambda x: pd.to_numeric(x, errors = 'coerce')).dropna().astype(int)

#ratings file
ratings = pd.read_csv(f'{cwd}/ml-latest-small/ratings.csv')
#convert timestamp to datetime and drop timestamp column
ratings['datetime'] = pd.to_datetime(ratings['timestamp'], unit='s')

#tags file
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


# -------------------------- RUN SQL AND EXPORT CSV -------------------------- #
#statement find top 3 or less mostly voted users for each genre (excluded input from CLI) for given via CLI year
query = """SELECT *
    FROM(
        SELECT userId, 
            COUNT(userId) AS user_count, 
            movies.movieId, 
            title, 
            year, 
            rating, 
            genres,
            row_number() OVER (PARTITION BY genres ORDER BY COUNT(userId) DESC) AS user_rank
        FROM movies 
        LEFT JOIN ratings ON ratings.movieId = movies.movieId 
        WHERE year = :year AND genres != :genre
        GROUP BY genres, userId
    )
WHERE user_rank <= 3"""

run_query_and_export(connection, query, year_cli, genre_cli)

connection.commit()
connection.close()
print('Database connection closed')