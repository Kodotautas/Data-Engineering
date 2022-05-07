import os
import random 
import pandas as pd
import sqlite3


# Task 5
# 1. Take your OLTP and OLAP from HW#4 and ml-latest-small.zip dataset.
# 2. Develop an application which generates nicknames for users in ratings.csv and loads those into a separate table in OLTP.
# 4. Develop an app, which will prompt a user on their ID and suggest to change their nickname.
# 5. Develop an app, which will extract only lately changed users and load those to OLAP. Chose Slowly Changing Dimension type and be ready to explain your choice. 
# 6. Similarly to HW#2, build a view of top-3 actively voting users per each genre for a given year, but this time with nicknames. 
# 7. Make sure that user updates from #3 are reflected in the results of #5

# --------------------------------- FUNCTIONS -------------------------------- #
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn


# --------------------------------- DATABASE --------------------------------- #
conn = create_connection('movies.db')
#read ratings table
users_df = pd.read_sql_query('''SELECT DISTINCT userId FROM ratings''', conn)

# --------------------------------- NICKNAMES -------------------------------- #
#heroes names from Github list
heroes_df = pd.read_json('https://raw.githubusercontent.com/sindresorhus/superheroes/main/superheroes.json').rename(columns={0:'hero'})

nicknames = [heroes_df.sample() for k in users_df.index]

print(nicknames)
#TO DO: extend heroes list and merge on index, column