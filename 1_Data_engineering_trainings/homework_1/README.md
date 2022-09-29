# Homework_1 App

## What App do?
1. Read movies.csv file which located in system
2. Dataframe can be filtered by year or by genre
3. Results are printed to console and exported as parquet format file 

## Main functions / arguments
`export_parquet(df)` - export parquet file to cwd/parquet directory. `df` - filtered dataframe

Arguments:
```Python
parser.add_argument('-Y', '--year', dest='year', type=int, nargs='+', help='Write one or multiple years. e.g. 2014 2017')

parser.add_argument('-G','--genre', dest='genre', type=str, choices=list_of_genres, help='Write movie genre or multiple genres. e.g. Drama')
```

## How to use it?
### Get filtered by year(s):
in terminal write: 
`"python app.py -Y 2017"` - single year filter or `"python app.py -Y 2017 2018 2019"` - multiple years filter

### Get filtered by genre(s):
in terminal write: 
`"python app.py -G Drama"` - single genre filter





