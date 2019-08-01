# /usr/local/Cellar/postgresql/11.1

# Steps to create DB
psql postgres
CREATE ROLE eyeota WITH LOGIN PASSWORD '3y3otaSG!!';
ALTER ROLE eyeota CREATEDB;
\q
psql postgres -U eyeota
CREATE DATABASE eyeota;
\connect eyeota

# username = eyeota
# password = 3y3otaSG!!
# database = eyeota

# https://www.codementor.io/engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb#iii-getting-started

# Table Schema (2 tables - s3_file_match_new, s3_file_match_old)
CREATE TABLE s3_file_match(
    id serial PRIMARY KEY,
    bucket VARCHAR(50) NOT NULL,
    platform VARCHAR(50) NOT NULL,
    file_name VARCHAR(200) NOT NULL,
    created_on TIMESTAMP NOT NULL,
    created_by VARCHAR(100) NOT NULL
);

select * from s3_file_match;