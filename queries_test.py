import psycopg2
import config

# Read credentials
dbname = config.postgres_credentials['dbname']
user = config.postgres_credentials['user']
password = config.postgres_credentials['password']
host = config.postgres_credentials['host']

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

# cursor.execute('SELECT * FROM zno_data WHERE "mathBall100" = 200;')  # 1176, 7319, 9029
# data = cursor.fetchall()
# for i in data:
#     print(i)

# cursor.execute('UPDATE zno_data SET "mathBall100" = 142 WHERE "mathBall100" = 200')

cursor.execute('''SELECT 
                    "Birth", 
                    AVG("mathBall100") as "Average_math_exam"
                FROM zno_data 
                WHERE "mathBall100" IS NOT NULL
                GROUP BY "Birth" 
                ORDER BY "Birth";''')
data = cursor.fetchall()
for i in data:
    print(i)

conn.commit()
cursor.close()
conn.close()
