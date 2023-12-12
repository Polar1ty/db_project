import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import config
import requests
import os
import py7zr

# Read credentials
dbname = config.postgres_credentials['dbname']
user = config.postgres_credentials['user']
password = config.postgres_credentials['password']
host = config.postgres_credentials['host']

# Download zno data file
req = requests.get('https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2019.7z', stream=True)
if req.status_code == 200:
    filename = 'zno_data_archive'
    with open(filename, 'wb') as file:
        file.write(req.content)

    os.rename(filename, f'{filename}.7z')

    with py7zr.SevenZipFile(f'{filename}.7z', mode='r') as archive:
        archive.extractall()

    if os.path.exists(f'{filename}.7z'):
        os.remove(f'{filename}.7z')
else:
    print(f'Request failed because of --> {req.status_code} {req.reason}')

raw_data = pd.read_csv('Odata2019File.csv', sep=";", decimal=",", encoding="Windows-1251", nrows=10000, low_memory=False)

zno_data = pd.DataFrame(raw_data,
                        columns=['Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME',
                                 'TerTypeName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName', 'EOTerName',
                                 'UkrTestStatus', 'mathTestStatus', 'mathBall100', 'mathBall12', 'mathBall'])

# Connect via sqlachemy for pd.to_sql
conn_string = f'postgresql://{user}:{password}@{host}/'
db = create_engine(conn_string)
conn = db.connect()

zno_data.to_sql('zno_data', con=conn, if_exists='replace', index=True)

conn.close()
db.dispose()

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

cursor.execute('SELECT * FROM zno_data WHERE "mathBall100" = 200;')
data = cursor.fetchall()
for i in data:
    print(i)

conn.commit()
cursor.close()
conn.close()
