## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_URL") # paste connection string here or read from .env file

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """CREATE TABLE IF NOT EXISTS podcast (
id TEXT PRIMARY KEY,
title TEXT
)"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """CREATE TABLE IF NOT EXISTS podcast_segment (
id text PRIMARY KEY,
start_time REAL,
end_time REAL,
content TEXT,
embedding VECTOR(128),
podcast_id TEXT REFERENCES podcast(id)
)"""

with psycopg2.connect(CONNECTION) as conn:
    with conn.cursor() as cur:
        cur.execute(CREATE_EXTENSION)
        cur.execute(CREATE_PODCAST_TABLE)
        cur.execute(CREATE_SEGMENT_TABLE)
        conn.commit()
        print("Tables created successfully")




