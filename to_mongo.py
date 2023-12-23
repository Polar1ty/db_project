import psycopg2
from pymongo import MongoClient
import config

# Read credentials
dbname = config.postgres_credentials['dbname']
user = config.postgres_credentials['user']
password = config.postgres_credentials['password']
host = config.postgres_credentials['host']

pg_connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
pg_cursor = pg_connection.cursor()

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['my_mongo_db']

# Migrate data from zno_data_new
pg_cursor.execute("SELECT * FROM zno_data_new")
zno_data_new_records = pg_cursor.fetchall()

mongo_zno_data_new_collection = mongo_db['zno_data_new']

# Define the column names
column_names = ['Birth', 'SEXTYPENAME', 'mathTestStatus', 'mathBall100', 'mathBall12', 'mathBall', 'location_id',
                'school_id', 'index']

# Insert the records
mongo_zno_data_new_collection.insert_many(
    {column: value for column, value in zip(column_names, record)} for record in zno_data_new_records)

# Migrate data from location_mapping
pg_cursor.execute("SELECT * FROM location_mapping")
location_mapping_records = pg_cursor.fetchall()

mongo_location_mapping_collection = mongo_db['location_mapping']

# Define the column names for location_mapping
location_mapping_column_names = ['location_id', 'AREANAME', 'TERNAME', 'REGTYPENAME', 'TerTypeName']

# Insert the records
mongo_location_mapping_collection.insert_many(
    {column: value for column, value in zip(location_mapping_column_names, record)} for record in
    location_mapping_records)

# Migrate data from school
pg_cursor.execute("SELECT * FROM school")
school_records = pg_cursor.fetchall()

mongo_school_collection = mongo_db['school']

# Define the column names for school
school_column_names = ['school_id', 'EONAME', 'EOTYPENAME']

# Insert the records
mongo_school_collection.insert_many(
    {column: value for column, value in zip(school_column_names, record)} for record in school_records)
