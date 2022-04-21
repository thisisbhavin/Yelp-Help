from os import getenv
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class GCP:
    BUCKET = getenv('GCP_BUCKET')
    FILE_URL = getenv('GCP_FILE_URL')

class Postgres:
    PG_DBNAME = getenv('PG_DBNAME', 'yelp')
    PG_HOST = getenv('PG_HOST', 'localhost')
    PG_PORT = getenv('PG_PORT', 5432)
    PG_USER = getenv('PG_USER', 'postgres')
    PG_PWD = getenv('PG_PWD', 'postgres')
    PG_TABLE_NAME = getenv("PG_TABLE_NAME", 'restaurants_info')

class Crawlera:
    CRAWLERA_APIKEY = getenv('CRAWLERA_APIKEY', 'paste your key here')
