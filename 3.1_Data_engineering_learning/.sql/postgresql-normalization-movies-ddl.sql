CREATE TABLE IF NOT EXISTS movies 
(
    movie_id SERIAL,
    title_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    original_title VARCHAR NOT NULL,
    duration_minutes integer NOT NULL DEFAULT 0,
    avg_vote REAL NOT NULL DEFAULT 0,
    count_vote integer NOT NULL DEFAULT 0,
    date_published date,
    production_company VARCHAR,
    description VARCHAR,
    budget_usd bigint NOT NULL DEFAULT 0,
    usa_gross_income_usd bigint NOT NULL DEFAULT 0,
    worldwide_gross_income_usd bigint NOT NULL DEFAULT 0,
    metascore REAL NOT NULL DEFAULT 0,
    reviews_from_users REAL NOT NULL DEFAULT 0,
    reviews_from_critics REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (movie_id),
    UNIQUE (title_id)
);

CREATE TABLE IF NOT EXISTS movie_numeric_votes
(
    vote_id serial,
    movie_id integer NOT NULL,
    rating_id integer NOT NULL,
    vote_count integer NOT NULL DEFAULT 0,
    PRIMARY KEY (vote_id)
);

CREATE TABLE IF NOT EXISTS movie_avg_votes
(
    vote_id serial,
    movie_id integer NOT NULL,
    rating_id integer NOT NULL,
    vote_avg REAL NOT NULL DEFAULT 0,
    vote_count integer NOT NULL DEFAULT 0,
    PRIMARY KEY (vote_id)
);

CREATE TABLE IF NOT EXISTS lookup_hdr
(
    lookup_hdr_id SERIAL,
    lookup_type VARCHAR NOT NULL,
    PRIMARY KEY (lookup_hdr_id),
    UNIQUE (lookup_type)
);

CREATE TABLE IF NOT EXISTS lookup_dtl
(
    lookup_dtl_id SERIAL,
    lookup_hdr_id integer NOT NULL,
    lookup_code VARCHAR NOT NULL,
    lookup_description VARCHAR,
    start_active_date date NOT NULL,
    end_active_date date NOT NULL,
    PRIMARY KEY (lookup_dtl_id),
    UNIQUE (lookup_hdr_id, lookup_code)
);

CREATE TABLE IF NOT EXISTS people
(
    person_id SERIAL,
    name_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    birth_name VARCHAR NOT NULL,
    spouse_count SMALLINT NOT NULL DEFAULT 0,
    divorce_count smallint,
    spouse_with_children_count smallint,
    children smallint,
    spouse_desc VARCHAR,
    bio VARCHAR,
    birth_date date,
    death_date date,
    birth_place VARCHAR,
    death_place VARCHAR,
    death_reason VARCHAR,
    PRIMARY KEY (person_id),
    UNIQUE (name_id)
);

CREATE TABLE IF NOT EXISTS movies_directors
(
    movie_id integer NOT NULL,
    person_id integer NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_genres
(
    movie_id integer NOT NULL,
    lookup_dtl_id integer NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_countries
(
    movie_id integer NOT NULL,
    lookup_dtl_id integer NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_languages
(
    movie_id integer NOT NULL,
    lookup_dtl_id integer NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_writers
(
    movie_id integer NOT NULL,
    person_id integer NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_actors
(
    movie_id integer NOT NULL,
    person_id integer NOT NULL
);

ALTER TABLE lookup_dtl
    ADD FOREIGN KEY (lookup_hdr_id)
    REFERENCES lookup_hdr (lookup_hdr_id)
    NOT VALID;

ALTER TABLE movies_directors
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;


ALTER TABLE movies_directors
    ADD FOREIGN KEY (person_id)
    REFERENCES people (person_id)
    NOT VALID;


ALTER TABLE movies_genres
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;


ALTER TABLE movies_genres
    ADD FOREIGN KEY (lookup_dtl_id)
    REFERENCES lookup_dtl (lookup_dtl_id)
    NOT VALID;


ALTER TABLE movies_countries
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;


ALTER TABLE movies_countries
    ADD FOREIGN KEY (lookup_dtl_id)
    REFERENCES lookup_dtl (lookup_dtl_id)
    NOT VALID;


ALTER TABLE movies_languages
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;


ALTER TABLE movies_languages
    ADD FOREIGN KEY (lookup_dtl_id)
    REFERENCES lookup_dtl (lookup_dtl_id)
    NOT VALID;


ALTER TABLE movies_writers
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;


ALTER TABLE movies_writers
    ADD FOREIGN KEY (person_id)
    REFERENCES people (person_id)
    NOT VALID;


ALTER TABLE movies_actors
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;


ALTER TABLE movies_actors
    ADD FOREIGN KEY (person_id)
    REFERENCES people (person_id)
    NOT VALID;
   
   
ALTER TABLE movie_numeric_votes
    ADD FOREIGN KEY (rating_id)
    REFERENCES lookup_dtl (lookup_dtl_id)
    NOT VALID;

   
ALTER TABLE movie_numeric_votes
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;
   
   
ALTER TABLE movie_avg_votes
    ADD FOREIGN KEY (rating_id)
    REFERENCES lookup_dtl (lookup_dtl_id)
    NOT VALID;

ALTER TABLE movie_avg_votes
    ADD FOREIGN KEY (movie_id)
    REFERENCES movies (movie_id)
    NOT VALID;
