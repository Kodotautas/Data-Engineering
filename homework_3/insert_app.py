import argparse
import os
import argparse
import pandas as pd
import sqlite3
import numpy as np

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

# ---------------------------- ARGPARSER & INPUTS --------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-R', '--rating', dest='rating', type=int, action='append', nargs='+', help='Rating. -> -R userId movieId rating')
parser.add_argument('-M','--movie', dest='movie', type=str, nargs='+', help="Movie. -> -M movieId 'title(release year)' genre")

args = parser.parse_args()

ratings_values = []
for _, value in parser.parse_args()._get_kwargs():
    if value is not None:
        print(value)
        ratings_values.append(value)

print(ratings_values)
#rating list and getter

# ratings_input()

#movies list and getter
movies_values = []
# movies_input()



# -------------------------- TRANSFER TO OLTP TABLES ------------------------- #
# connection = sqlite3.connect('movies.db')
# cursor = connection.cursor()

# TO DO: allow multiple inputs from CLI 
# or append every time after new entry
