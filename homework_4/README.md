# Homework_4 App

## What App do?
1. App use OLTP from HW#3 and ml-latest-small.zip, bad words list datasets. 
2. Reads database (tags.csv was read previously)
3. Create a reports(sql views) on movies, containing: 
 		a.Average rating for movie per genre
 		b.Count of existing tags (metric describing the will of people to discuss the movie)
 		c.Total word count of tags (metric on opinion expression intensity) 
 		d.Mark movie's page which should be moderated better (tags contain sensitive or obscene language)

## Main functions / arguments
`def create_connection(db_file)` - function connect db from homework_3. (`db_file` - database file path.

## How to use it?
run file in terminal: `"python app.py"` or text editor.




