--create links table
CREATE TABLE IF NOT EXISTS links
(movieID INT, imdbId INT, tmdbId INT);

--create movies table
CREATE TABLE IF NOT EXISTS movies
(movieId INT, title TEXT, year INT, genres TEXT);

--create movies table
CREATE TABLE IF NOT EXISTS ratings
(userId INT, movieId INT, rating INT, timestamp INT, datetime TEXT);

--create tags table
CREATE TABLE IF NOT EXISTS tags
(userId INT, movieId INT, tag TEXT, timestamp INT, datetime TEXT);
