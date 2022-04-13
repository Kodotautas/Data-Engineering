import requests, zipfile, io
import os
import pandas as pd
import re

# ----------------------------- SHORT DESCRIPTION ---------------------------- #
# app download zip file from grouplens
# read csv file about movies
# extract movies release year & main genre
# by user inputs full dataframe is filtered by year and/or main genre
# results are printed to console and exported as parquet format file 

#get working directory
cwd = os.getcwd()
print(cwd)

# --------------------------- READ & SAVE DATAFRAME -------------------------- #
# url link
url = 'https://files.grouplens.org/datasets/movielens/ml-latest-small.zip'

#read & extract zip file
r = requests.get(url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall()

#create dataframe from file
df = pd.read_csv(f"{cwd}/ml-latest-small/movies.csv", sep=',')


# ---------------------------------- FILTER ---------------------------------- #
#get year, clean
# (TO DO: clean full or use regex \(\d{4}\))
df['year'] = df['title'].str[-5:].str[:-1]
df['year'] = df['year'].apply(lambda x: pd.to_numeric(x, errors = 'coerce')).dropna().astype(int)

#find earliest and newest movie by year
min_year = int(df['year'].min())
max_year = int(df['year'].max())

#get main genres
df['main_genre'] = df['genres'].str.extract(r'([^|]*)')
#remove no genre movies
df = df[df['main_genre'] != '(no genres listed)']

#generate unique main genres values
genres = df['main_genre'].unique()


# -------------------------------- USER INPUTS / APP ------------------------------- #
#functions
def export_parquet(df):
    """function export parquet file to cwd/parquet directory

    Args:
        df (dataframe): filtered dataframe by user input
    """    
    df.to_parquet(f"{cwd}/parquet/movies.parquet.gzip'", compression='brotli')  
    print('Parquet file exported')


#ask user input choices, print values and save as Parquet
while True:
    need_year = input("Would you like choose movies year? (yes/no) ")
    if need_year == 'yes':
        try:
            year = int(input(f"What year movie looking? Choose from {min_year} to {max_year}: "))
        except ValueError:
            print('Please enter number.')
            continue
        need_genre = input("Would you like choose movie genre? (yes/no) ")
        if need_genre == 'yes':
            print('You can choose main genres: ','\n'.join(genres))
            genre_choose = input("Write genre: ")
            print('Please choose available main genre.')
            user_df = df[(df['year'] == year) & (df['main_genre'] == genre_choose)]
            print(user_df)
            export_parquet(user_df)
            break
        elif need_genre == 'no':
            user_df = df[df['year'] == year]
            print(user_df)
            export_parquet(user_df)
            break
        else:
            print('Program stopped please check input requirements')


    if need_year == 'no':
        need_genre = input("Would you like choose movie genre? (yes/no) ")
        if need_genre == 'yes':
            print('You can choose main genres: ','\n'.join(genres))
            genre_choose = input("Write genre: ")
            user_df = df[df['main_genre'] == genre_choose]
            print(user_df)
            export_parquet(user_df)
            break
        elif need_genre == 'no':
            print("You didn't choose any values. App is closed")
            break
        else:
            print('Program stopped please check it requirements')
            
