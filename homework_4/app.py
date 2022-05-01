import os
import pandas as pd
import sqlite3

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# Task 4
# 1.Take your OLTP from HW#3 and ml-latest-small.zip dataset. 
# 2.Develop an application which 
# 	1.Reads your DB
# 	2.Reads tags.csv 
# 	3.Builds a report(sql view) on movies, containing 
# 		1.avg rating
# 		2.count of existing tags (metric describing the will of people to discuss the movie)
# 		3.total word count of tags (metric on opinion expression intensity) 
# 		4.mark on whether movie page should be moderated better (tags contain sensitive or obscene language)
# 	4.Uploads the report your separate OLAP DB.
# 3.Prepare views that will allow your OLAP users analyze metrics 2.3.1-2.3.4 by genres
# 4.Extra: try to avoid using high-level frameworks like Spark and Pandas

#get cwd
cwd = os.getcwd()

# --------------------------------- FUNCTIONS -------------------------------- #
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn

def drop_view(view_name):
    conn.execute(f'''DROP VIEW IF EXISTS {view_name}''')
    
# ------------------------------ SQLITE SECTION ------------------------------ #
conn = create_connection('movies.db')

# ---------------------------------------------------------------------------- #
#                                    REPORTS                                   #
# ---------------------------------------------------------------------------- #
# ------------------------------ AVERAGE RATING ------------------------------ #
#view name and drop it if exists
movies_average_ratings = 'movies_average_ratings'
drop_view(movies_average_ratings)

#SQL statement
conn.execute(f'''
            CREATE VIEW {movies_average_ratings} AS
            SELECT 
                userId, movieId, avg(rating) as avg_rating, title, year, genres
            FROM(
                select *
                FROM ratings
                LEFT JOIN movies
                ON ratings.movieId = movies.movieId
                )
            GROUP BY title
            ORDER BY avg(rating) DESC
            ''')

conn.commit()
print(f'VIEW: {movies_average_ratings} is created.')

# -------------------------------- TAGS COUNT -------------------------------- #
#view name and drop it if exists
count_of_tags = 'count_of_tags'
drop_view(count_of_tags)

#SQL statement
conn.execute(f'''
            CREATE VIEW {count_of_tags} AS
            SELECT userId, movieId, count(tag) as count_tag, title, year, genres
            FROM(
                SELECT *
                FROM tags
                LEFT JOIN movies
                ON tags.movieId = movies.movieId
                )
            GROUP BY title
            ORDER BY count(tag) DESC
            ''')

conn.commit()
print(f'VIEW: {count_of_tags} is created.')

# -------------------------- COUNT OF DIFFERENT TAGS ------------------------- #
#view name and drop it if exists
count_of_different_tags = 'count_of_tags'
drop_view(count_of_different_tags)

#SQL statement
conn.execute(f'''
            CREATE VIEW {count_of_different_tags} AS
            SELECT userId, movieId, count(distinct(tag)) as count_distinct_tag, title, year, genres
            FROM(
                SELECT *
                FROM tags
                LEFT JOIN movies
                ON tags.movieId = movies.movieId
                )
            GROUP BY title
            ORDER BY count(distinct(tag)) DESC
            ''')

conn.commit()
print(f'VIEW: {count_of_different_tags} is created.')

# ----------------------- IS MOVIE SHOULD BE MODERATED? ---------------------- #
