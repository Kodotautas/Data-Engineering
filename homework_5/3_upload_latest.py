import os
import pandas as pd
import sqlite3

# Task 5
# 5. Develop an app, which will extract only lately changed users and load those to OLAP. Chose Slowly Changing Dimension type and be ready to explain your choice. 
# 6. Similarly to HW#2, build a view of top-3 actively voting users per each genre for a given year, but this time with nicknames. 
# 7. Make sure that user updates from #3 are reflected in the results of #5

cwd = os.getcwd()

# ----------------------------- CONNECT DATABASE ----------------------------- #

#create connection
oltp = sqlite3.connect(f'{cwd}/SQL_DB/reports_OLTP.db')
print('Connected OLTP')

olap = sqlite3.connect(f'{cwd}/SQL_DB/movies_OLAP.db')
print('Connected OLAP')

#get latest 3 users 
users_df = pd.read_sql_query('''SELECT * 
                                FROM users_nicknames
                                ORDER BY create_datetime DESC
                                LIMIT 3''', oltp)


# #export dataframe to db table
users_df.to_sql('users_nicknames', olap, if_exists='replace', index = False)
print('Latest user nicknames exported.')

#commit & close connection
oltp.close()
olap.close()