from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()


def create_database(database_name):
    # Connect to the 'postgres' default database to create 'stations'
    conn = psycopg2.connect(
        dbname="postgres",  # default DB
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
    )
    conn.autocommit = True  # Allow immediate execution of CREATE DATABASE
    cursor = conn.cursor()

    # Check if 'stations' database exists
    query = "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;"
    cursor.execute(query, (database_name,))

    exists = cursor.fetchone()

    if not exists:
        # Create 'stations' database if it does not exist
        cursor.execute(f"CREATE DATABASE {database_name};")
        print("Database '{database_name}' created.")
    else:
        print(f"Database '{database_name}' already exists")

    cursor.close()
    conn.close()
