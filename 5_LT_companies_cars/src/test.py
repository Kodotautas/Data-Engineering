# script read csv file and transform

import pandas as pd

def read_csv_file(file_name, delimiter):
    df = pd.read_csv(file_name, delimiter=delimiter)
    return df

df = read_csv_file('/home/vytautas/Desktop/lithuania_statistics_VidausVandenuLaivas.csv', ',')
# leave column ship_constructed
df = df[['ship_constructed']]
df = df.dropna()

# convert like 1990.0 to 1990
df['ship_constructed'] = df['ship_constructed'].astype(str).str[:-2]
# convert ship_constructed to yyyy-mm-dd
df['ship_constructed'] = pd.to_datetime(df['ship_constructed'], format='%Y')

# rint columan name and type
print(df.head())
print(df.dtypes)
