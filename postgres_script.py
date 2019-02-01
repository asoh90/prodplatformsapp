# /usr/local/Cellar/postgresql/11.1

# https://www.codementor.io/engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb#iii-getting-started

# Table Schema (2 tables - s3_file_match_new, s3_file_match_old)
# CREATE TABLE s3_file_match_new(
#     id serial PRIMARY KEY,
#     bucket VARCHAR(50) NOT NULL,
#     file_name VARCHAR(200) NOT NULL,
#     created_on TIMESTAMP NOT NULL
# );

# Insert query
INSERT_QUERY = "INSERT INTO s3_file_match(bucket, file_name, platform, created_on, created_by) values ('{}', '{}', '{}', NOW(), '{}')"
GET_ID_QUERY = "SELECT id FROM s3_file_match where bucket='{}' and file_name='{}'"

# Portover query
DELETE_ID_QUERY = "delete from s3_file_match where not id in (select id from s3_file_match order by id desc limit {})"

COUNT_RECORDS_QUERY = "SELECT COUNT(*) FROM s3_file_match"

GET_LINK_QUERY = "SELECT bucket, file_name FROM s3_file_match where id='{}'"
GET_FILE_NAME_QUERY = "SELECT file_name FROM s3_file_match where id='{}'"

GET_ALL_QUERY = "SELECT * FROM s3_file_match order by id desc"