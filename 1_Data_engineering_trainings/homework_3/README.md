# Homework_3 App

## What App do?
1. Read csv files from ml-latest-small folder (data about movies)
2. Extract movies year, genres and normalize genres column
3. Uploads all data to database tables
4. App receives either new ratings or new movies (via CLI dialogue)
5. Populates proper OLTP tables with data from user
6. App check edge cases: is year not string, extract year from brackets, capitalize title and genres

## Main functions / arguments
`def sql_inserter(db_name, sql_statement)` - function connect db and run sql statement. (`db_name` - database name. `sql_statement` - SQL query which function will run.)

Arguments:
```Python
parser.add_argument('-R', '--rating', dest='rating', type=int, nargs='+', help='Rating. -> -R INT[userId] INT[movieId] INT[rating]')

parser.add_argument('-M','--movie', dest='movie', type=str, nargs='+', help="Movie. -> -M INT[movieId] STR['title(release year)'] STR['genre']")
```

## How to use it?
### Insert movie rating or movies data:
`"python insert_app.py -R 10 15 5'"` - add rating of movie

`"python insert_app.py -M 15 'Programmer (2022) 'Fantasy|Comedy"` - add movie(s) data to database movies table




