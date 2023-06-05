import argparse
import os
import argparse
import pandas as pd
import sqlite3
import datetime

# ---------------------------------- SUMMARY --------------------------------- #
# Aplication allow user write userId and change their nickname.
# New nickname is uploaded in SQL table with timestamp info
#

cwd = os.getcwd()

# --------------------------------- FUNCTIONS -------------------------------- #
def new_nickname_generator(userid, nicknames_df_column):
    """function generates nicknames
    Args:
        userid (int): uses number
        nicknames_df_column (dataframe_column): dataframe and it's column where     Returns:
        string: new nickname of user
    """
    new_nickname = nicknames_df_column.sample().values + ' ' + str(userid)
    new_nickname = new_nickname[0]
    return new_nickname

def append_nicknames(file_name, text_to_append):
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        # Move read cursor to the start of file.
        file_object.seek(0)
        # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0:
            file_object.write("\n")
        # Append text at the end of file
        file_object.write(text_to_append)

# --------------------------------- ARGPARSER -------------------------------- #
# init parser
parser = argparse.ArgumentParser(description="Change user nickname by their id.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

#parse command line arguments
parser.add_argument('-U', '--userid', dest='userid', type=int, help='Userid. -> -U 101')
# parser.add_argument('-E', '--export', dest='export', nargs='?', help='Export all ') #launch nicknames batch export
args = parser.parse_args()

# -------------------------------- CONNECTION -------------------------------- #
#create connection
conn = sqlite3.connect(f'{cwd}/SQL_DB/reports_OLTP.db')
print('Connected database')

#get users id and their nicknames from table
users_df = pd.read_sql_query('''SELECT userId, nickname 
                             FROM users_nicknames''', conn)


# --------------------------- USER NICKNAME CHANGER -------------------------- #
#heroes names list from Github
heroes_df = pd.read_json('https://raw.githubusercontent.com/sindresorhus/superheroes/main/superheroes.json', orient='records').rename(columns={0:'hero'})

#after user input userId
if args.userid:
    for value in parser.parse_args()._get_kwargs():
        userid = value[1]
        
        #get current user nickname
        user_nickname = (pd.read_sql_query(f'''SELECT nickname FROM users_nicknames WHERE userID = {userid}''', conn)).values[0][0]
        print(f'User: {userid} current Nickname: {user_nickname}')
        
        #generate new name
        new_nickname = new_nickname_generator(userid, heroes_df['hero'])
        print(f'User: {userid} new Nickname: {new_nickname}')    

        #get current datetime
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        
        #values to joined list    
        # values = [userid, new_nickname, dt]
        # values = ', '.join(str(value) for value in values)
        # print(userid, new_nickname, dt)
        
        conn.execute(f'''UPDATE users_nicknames
                        SET nickname = '{new_nickname}', 
                        create_datetime = '{dt}'
                        WHERE userId = {userid}''')
        
        
#commit & close connection
conn.commit()
conn.close()
print('User nickname changed & connection closed')