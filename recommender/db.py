import os
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_URL") # paste connection string here or read from .env file
