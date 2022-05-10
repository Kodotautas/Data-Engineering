import argparse
import os
import argparse
import pandas as pd
import sqlite3
from nicknames import * #import functions from another .py

# ---------------------------------- SUMMARY --------------------------------- #
# 4. Develop an app, which will prompt a user on their ID and suggest to change their nickname.

# 5. Develop an app, which will extract only lately changed users and load those to OLAP. Chose Slowly Changing Dimension type and be ready to explain your choice. 
# 6. Similarly to HW#2, build a view of top-3 actively voting users per each genre for a given year, but this time with nicknames. 
# 7. Make sure that user updates from #3 are reflected in the results of #5

# --------------------------------- FUNCTIONS -------------------------------- #


# --------------------------------- ARGPARSER -------------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Filter movies.csv data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-U', '--userid', dest='userid', type=int, help='Userid. -> -U ????????????????????/')

args = parser.parse_args()

# ---------------------------------- DATABASE --------------------------------- #
#create connection
conn = create_connection('movies.db')

#get users it and heir nicknames from table
users_df = pd.read_sql_query('''SELECT userId, nickname FROM users_nicknames''', conn)