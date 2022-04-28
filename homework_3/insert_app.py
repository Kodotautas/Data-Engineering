import argparse
import os
import argparse
import sqlite3
import numpy as np
import calendar
import time
from datetime import datetime
from functools import reduce
import re
import copy


# ----------------------------- SHORT DESCRIPTION ---------------------------- #

# App receives either new ratings or new movies (via CLI dialogue)
# Populates proper OLTP tables with data from user
# App check edge cases: is year not string, capitalize title and genres

#get working directory
cwd = os.getcwd()


# --------------------------------- FUNCTIONS -------------------------------- #
def get_timestamp():
    """function generates current timestamp
    Returns:
        str: the corresponding Unix timestamp value
    """    
    gmt = time.gmtime()
    ts = calendar.timegm(gmt)
    return ts

def to_tuple(lst):
    """function convert to 
    Args:
        lst (list): data list which need to convert
    Returns:
        tuple: returns converted tuple
    """    
    return tuple(to_tuple(i) if isinstance(i, list) else i for i in lst)

def sql_inserter(db_name, sql_statement):
    """function connect db and run sql statement

    Args:
        db_name (database, str): database name which to connect, like 'movie.db' 
        sql_statement (): SQL statement which need to run
    """ 
    try:
        sqliteConnection = sqlite3.connect(db_name)
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")

        sqlite_insert_query = sql_statement

        count = cursor.execute(sqlite_insert_query)
        sqliteConnection.commit()
        print("Record inserted successfully into table ", cursor.rowcount)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

def duplicate(copyList, n):
    """function n times copy list items to create it nested or not
    Args:
        copyList (list): list name
        n (int): how many times copy list
    Returns:
        list: multiple times copied list
    """    
    return [copy.deepcopy(ele) for ele in copyList for _ in range(n)] 

# ----------------------------- CONNECT DATABASE ----------------------------- #
connection = sqlite3.connect('movies.db')
cursor = connection.cursor()


# ---------------------------- ARGPARSER & INPUTS --------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-R', '--rating', dest='rating', type=int, nargs='+', help='Rating. -> -R INT[userId] INT[movieId] INT[rating]')
parser.add_argument('-M','--movie', dest='movie', type=str, nargs='+', help="Movie. -> -M INT[movieId] STR['title(release year)'] STR['genre']")

args = parser.parse_args()

#get time stamp and convert it to datetime
ts = get_timestamp()
dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------- ADD RATING SECTION --------------------------- #
ratings_values = [] #get from CLI

#process ratings
if args.rating:
    for _, value in parser.parse_args()._get_kwargs():
        if value is not None:
            ratings_values.append(value)
    
    #append to ratings values list
    ratings_values[0].append(ts)
    ratings_values[0].append(dt)

    #combine to one list and convert to tuple
    ratings_values = to_tuple(reduce(lambda x,y: x+y, ratings_values))
    print('Final ratings data: ', ratings_values)

    #ratings data SQL statement
    rating_insert_query = f'''INSERT INTO ratings
                            (userId, movieId, rating, timestamp, datetime) 
                            VALUES {ratings_values}'''
            
    sql_inserter('movies.db', rating_insert_query)
    print('Ratings added.')
    
    
# ---------------------------- ADD MOVIE SECTION --------------------------- #
movies_values = [] #get from CLI

# process movie
if args.movie:
    for _, value in parser.parse_args()._get_kwargs():
        if value is not None:
            movies_values.append(value)
        

    # ------------------------------ SOME EDGE CASES ----------------------------- #
    #save as variables from year, genre inputs
    #movieId
    try:
        movieid_cli = int(args.movie[0])
    except ValueError:
        print('MovieId must be an integer')
        movies_values = []
        print('Please give correct input')
    
    #title
    title_cli = str(args.movie[1]).capitalize() #first cap letter
    if bool(re.search('(\(\d{4}\))', title_cli)) is False:
        print('Title must be with year in round brackets () in the end. Please give correct input')
        movies_values = []
    
    #get year
    year = re.search('(\(\d{4}\))', title_cli)[0]
    year = int(year[1:-1])
    
    #genres 
    genres_cli = str(args.movie[2])
    #delimit data by: , | . ; symbols
    genres = re.split("[, \|.;]", genres_cli)
    #capitalize each genre
    for i in range(len(genres)):
        genres[i] = genres[i].capitalize()    
    
    #construct final values
    final_movies_values = []
    final_movies_values.append(movieid_cli)
    final_movies_values.append(title_cli)
    final_movies_values.append(year)
  
    #create multiple lists
    final_movies_values = duplicate([final_movies_values], len(genres))
    
    #add separate genres to list
    for i in range(len(genres)):
        final_movies_values[i].insert(i + 10, genres[i])
            
    if len(genres) != 1:
        final_movies_values = to_tuple(reduce(lambda x,y: x+y, final_movies_values))
        #split whole tuple to nested tuple
        final_movies_values = str(tuple(final_movies_values[x:x + 4] for x in range(0, len(final_movies_values), 4)))[1:-1]

    else:
        final_movies_values = to_tuple(reduce(lambda x,y: x+y, final_movies_values))
            
    print('Your final input is: ', '\n', final_movies_values)
    
     
    #movies data SQL statement
    movies_insert_query = f'''INSERT INTO movies
                             (movieId, title, year, genres) 
                             VALUES {final_movies_values}'''
    

    sql_inserter('movies.db', movies_insert_query)
    print('Movies data added.')