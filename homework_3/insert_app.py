import argparse
import os
import argparse
import pandas as pd
import sqlite3
import numpy as np
import calendar
import time
from datetime import datetime

# ----------------------------- SHORT DESCRIPTION ---------------------------- #

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

# def ratings_input():
#     """function get ratings data inputs from CLI and save values to list
#     Returns:
#         list: rating data"""
#     try:
#         userid = args.rating[0]
#         movieid = args.rating[1]
#         rating = args.rating[2]
#         print('Ratings data: ')
#         print(f"'userId: {userid}', 'movieId: {movieid}', 'rating: {rating}'")
#         ratings_values.extend([userid, movieid, rating])
#         print('Movies data added to list')
#     except TypeError:
#         pass
#     return ratings_values

# def movies_input():
#     """function get movies data inputs from CLI and save values to list
#     Returns:
#         list: movies data"""
#     try:
#         movieid = int(args.movie[0])
#         title = args.movie[1]
#         genre = args.movie[2:]
#         print('Movies data: ')
#         print(f"'movieId: {movieid}', 'title: {title}', 'genre: {genre}'")
#         ratings_values.extend([movieid, title, genre])
#         print('Movies data added to list')
#     except TypeError:
#         pass
#     return ratings_values

# ----------------------------- CONNECT DATABASE ----------------------------- #
connection = sqlite3.connect('movies.db')
cursor = connection.cursor()


# ---------------------------- ARGPARSER & INPUTS --------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-R', '--rating', dest='rating', type=int, action='append', nargs='+', help='Rating. -> -R userId movieId rating')
parser.add_argument('-M','--movie', dest='movie', type=str, nargs='+', help="Movie. -> -M movieId 'title(release year)' genre")

args = parser.parse_args()


# ---------------------------- ADD RATINGS SECTION --------------------------- #
#ratings
ratings_values = [] #get from CLI

for _, value in parser.parse_args()._get_kwargs():
    if value is not None:
        # print(value)
        ratings_values.append(value)
    
#get time stamp and convert it to datetime
ts = get_timestamp()
dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

#append timestamp and datetime to list items
if range(len(ratings_values) >= 0):
    for i in range(len(ratings_values) + 1):
        for item in ratings_values:
            try:
                item[i].append(ts)
                item[i].append(dt)
            except Exception as e: #error occurs if user input one row data
                ratings_values.append(ts)
                ratings_values.append(dt)


print('Ratings list:', ratings_values)
converted =  to_tuple(ratings_values)
print(converted)

#inser data to ratings table
try:
    sqliteConnection = sqlite3.connect('movies.db')
    cursor = sqliteConnection.cursor()
    print("Successfully Connected to SQLite")

    sqlite_insert_query = """INSERT INTO ratings
                          (userId, movieId, rating, timestamp, datetime) 
                           VALUES (22, 333, 7, 9999999, '2022-03-01 00:00:00')
                          """

    count = cursor.execute(sqlite_insert_query)
    sqliteConnection.commit()
    print("Record inserted successfully into ratings table ", cursor.rowcount)
    cursor.close()

except sqlite3.Error as error:
    print("Failed to insert data into sqlite table", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed")
        
