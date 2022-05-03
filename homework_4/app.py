import os
import pandas as pd
import sqlite3

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# App use OLTP from HW#3 and ml-latest-small.zip dataset. 
# Reads database (tags.csv was read previously)
# Create a reports(sql views) on movies, containing: 
# 		1.Average rating for movie per genre
# 		2.Count of existing tags (metric describing the will of people to discuss the movie)
# 		3.Total word count of tags (metric on opinion expression intensity) 
# 		4.Mark on whether movie page should be moderated better (tags contain sensitive or obscene language)
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
#load and insert to db bad words list from Github repository: "List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words"
bad_words = pd.read_csv(f'{cwd}/ml-latest-small/bad_words.csv')
bad_words.to_sql('bad_words', con=conn, if_exists = 'replace')

bad_words_filter = 'bad_words_filter'
drop_view(bad_words_filter)

#SQL statement
conn.execute(f'''
            CREATE VIEW {bad_words_filter} AS
            SELECT filter.movieId, title, year, lower_tag, category as status
            FROM(
                SELECT
                    movieId,
                    lower(tag) as lower_tag,
                    CASE
                        WHEN lower(tag) IN (SELECT bad_words_column FROM bad_words) THEN
                            'moderate'
                        ELSE
                            'OK'
                        END category
                FROM
                    tags
                ) AS filter
            LEFT JOIN movies
            ON filter.movieId = movies.movieId
            ORDER BY CATEGORY DESC
            ''')

conn.commit()
print(f'VIEW: {bad_words_filter} is created.')

# ----------------------------- CLOSE CONNECTION ----------------------------- #
conn.commit()
conn.close()
print('Views created & connection closed')