import os
import pandas as pd
import sqlite3

# ---------------------------------- SUMMARY --------------------------------- #
# App take  OLTP and OLAP from HW#4(ratings.csv was uploaded)
# Develop an application which generates nicknames for users from ratings tbale
# Uploads those into a separate table in OLTP.

cwd = os.getcwd()

#default action if table exists
action = 'fail'


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
#read ratings table ang unique user names
users_df = pd.read_sql_query('''SELECT DISTINCT userId FROM ratings''', conn)


# --------------------- GENERATE NICKNAMES AND LOAD TO DB -------------------- #
#heroes names list from Github
heroes_df = pd.read_json('https://raw.githubusercontent.com/sindresorhus/superheroes/main/superheroes.json', orient='records').rename(columns={0:'hero'})

#generate nicknames for users
users_df['nickname'] = users_df['userId'].map(lambda x: ' '.join((heroes_df['hero'].sample()).values) + ' ' + str(x))
users_df['nickname'] = users_df['nickname'].astype(str)

#load userId and nicknames to OLTP
try:
    users_df.to_sql('users_nicknames', con=conn, if_exists = action)
except ValueError:
    print("You want regenerate nicknames for users. Change action parameter to 'replace'.")

#close connection
conn.commit()
conn.close()
print('Database connection closed.')