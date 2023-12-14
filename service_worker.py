import psycopg2
import config

# Read credentials
dbname = config.postgres_credentials['dbname']
user = config.postgres_credentials['user']
password = config.postgres_credentials['password']
host = config.postgres_credentials['host']

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

# Drop existing tables for overwrite
drop_tables_query = """
DROP TABLE IF EXISTS location_mapping;
DROP TABLE IF EXISTS zno_data_new;
DROP TABLE IF EXISTS school;
"""
cursor.execute(drop_tables_query)
conn.commit()

# Create a new table for unique locations and their IDs
create_table_query = """
CREATE TABLE IF NOT EXISTS location_mapping (
    location_id SERIAL PRIMARY KEY,
    AREANAME VARCHAR(255),
    TERNAME VARCHAR(255),
    REGTYPENAME VARCHAR(255),
    TerTypeName VARCHAR(255)
);
"""
cursor.execute(create_table_query)
conn.commit()

# Populate the new table with unique locations
distinct_locations_query = """
INSERT INTO location_mapping ("areaname", "tername", "regtypename", "tertypename")
SELECT DISTINCT "AREANAME", "TERNAME", "REGTYPENAME", "TerTypeName"
FROM zno_data
WHERE "AREANAME" IS NOT NULL AND "TERNAME" IS NOT NULL AND "REGTYPENAME" IS NOT NULL AND "TerTypeName" IS NOT NULL;
"""
cursor.execute(distinct_locations_query)
conn.commit()

# Create a new table as a copy of zno_data with location_id column
create_new_table_query = """
CREATE TABLE IF NOT EXISTS zno_data_new AS
SELECT
    z.*,
    lm.location_id AS location_id
FROM
    zno_data z
    JOIN location_mapping lm ON
        z."AREANAME" = lm."areaname"
        AND z."TERNAME" = lm."tername"
        AND z."REGTYPENAME" = lm."regtypename"
        AND z."TerTypeName" = lm."tertypename";
"""
cursor.execute(create_new_table_query)
conn.commit()

# Drop the old location columns from the new table
drop_columns_query = """
ALTER TABLE zno_data_new
DROP COLUMN IF EXISTS "REGNAME",
DROP COLUMN IF EXISTS "AREANAME",
DROP COLUMN IF EXISTS "TERNAME",
DROP COLUMN IF EXISTS "REGTYPENAME",
DROP COLUMN IF EXISTS "TerTypeName";
"""
cursor.execute(drop_columns_query)
conn.commit()

# Create a new table for unique schools and their IDs
create_school_query = """
CREATE TABLE IF NOT EXISTS school (
    school_id SERIAL PRIMARY KEY,
    EONAME VARCHAR(255),
    EOTYPENAME VARCHAR(255)
);
"""
cursor.execute(create_school_query)
conn.commit()

# Populate the school table with unique schools
distinct_schools_query = """
INSERT INTO school ("eoname", "eotypename")
SELECT DISTINCT "EONAME", "EOTYPENAME"
FROM zno_data
WHERE "EONAME" IS NOT NULL AND "EOTYPENAME" IS NOT NULL;
"""
cursor.execute(distinct_schools_query)
conn.commit()

# Add School_ID column to zno_data_new table
alter_zno_data_new_query = """
ALTER TABLE zno_data_new
ADD COLUMN IF NOT EXISTS school_id INTEGER;
"""
cursor.execute(alter_zno_data_new_query)
conn.commit()

# Update the zno_data_new table with School_ID values
update_zno_data_new_query = """
UPDATE zno_data_new AS z
SET school_id = s.school_id
FROM school AS s
WHERE z."EONAME" = s."eoname" AND z."EOTYPENAME" = s."eotypename";
"""
cursor.execute(update_zno_data_new_query)
conn.commit()

# Drop the old school and other unnecessary columns from the new table
drop_columns_query = """
ALTER TABLE zno_data_new
DROP COLUMN IF EXISTS "EONAME",
DROP COLUMN IF EXISTS "EOTYPENAME",
DROP COLUMN IF EXISTS "EORegName",
DROP COLUMN IF EXISTS "EOAreaName",
DROP COLUMN IF EXISTS "EOTerName",
DROP COLUMN IF EXISTS "UkrTestStatus";
"""
cursor.execute(drop_columns_query)
conn.commit()

# Close the database connection
cursor.close()
conn.close()
