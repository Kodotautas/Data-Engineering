# Homework_2 App

## What App do?
1. Read csv files from ml-latest-small folder (all data about movies)
2. Uploads all data to SQLite database
3. Get a view of top-3 actively voting users per each genre for a given year (arguments via CLI)
4. Results are exported to csv file  

## Main functions / arguments
`def export_to_csv(df)` - export any dataframe to working directory. `df` - dataframe

`def run_query_and_export(conn, query, year_cli, genre_cli)` - run SQL query and results is exported to csv file

`conn` (str): connection to database
`query` (str): SQL query which function will run
`year_cli` (int): year input from CLI
`genre_cli` (str): genre (will be excluded) input from CLI 


Arguments:
```Python
parser.add_argument('-Y', '--year', dest='year', type=int, nargs='+', help='Write e.g. 2014')

parser.add_argument('-G','--genre', dest='genre', type=str, choices=list_of_genres, help="Write movie genre or multiple genres. e.g. 'Drama' or 'Crime|Drama'")
```

## How to use it?
### Get filtered by year & genre:
in terminal write: 
`"python app.py -Y 2017 -G 'Drama'"` - single year and genre filter

Results will be for 2017 year and 'Drama' genre will be excluded. 





