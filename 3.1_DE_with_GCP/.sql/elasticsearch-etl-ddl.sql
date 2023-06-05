CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_es_movie_principals; 

CREATE TABLE IF NOT EXISTS staging.stg_es_movie_principals(
	title_id varchar,
	title varchar,
	original_title varchar,
	duration_minutes integer,
	date_published timestamp,
	production_company varchar,
	description varchar,
	name_id varchar,
	"name" varchar,
	category varchar,
	job varchar,
	"character" varchar,
	created_date TIMESTAMP DEFAULT now(),
    last_updated_date TIMESTAMP DEFAULT now()
);
