import psycopg2
import os
# Load environment variables
db_host = os.getenv("DB_HOST", "localhost")
db_name = os.getenv("DB_NAME", "kolezza")
db_user = os.getenv("DB_USER", "kolezzians")
db_password = os.getenv("DB_PASSWORD", "kolezza")
# Connect to the database
connection = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password
)