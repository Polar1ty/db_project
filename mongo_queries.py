from pymongo import MongoClient

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['my_mongo_db']

# Query 1: Extract records from zno_data_new with mathBall100 in range (120, 160)
# query1 = {"mathBall100": {"$gte": 120, "$lte": 160}}
# result1 = mongo_db['zno_data_new'].find(query1)
# print("Query 1 Result:")
# for record in result1:
#     print(record)

# Query 2: Extract records from zno_data_new with Birth=1999
# query2 = {"Birth": 1999}
# result2 = mongo_db['zno_data_new'].find(query2)
# print("\nQuery 2 Result:")
# for record in result2:
#     print(record)

# Query 3: Extract records from school where eotypename='заклад вищої освіти'
# query3 = {"EOTYPENAME": "заклад вищої освіти"}
# result3 = mongo_db['school'].find(query3)
# print("\nQuery 3 Result:")
# for record in result3:
#     print(record)

# Query 4: Extract records from location_mapping with TerTypeName = 'село'
# query4 = {"TerTypeName": "село"}
# result4 = mongo_db['location_mapping'].find(query4)
# print("\nQuery 4 Result:")
# for record in result4:
#     print(record)

# Query 5: Extract records from zno_data_new joined by school_id with the school table (with limit 100)
pipeline = [
    {
        "$lookup": {
            "from": "school",
            "localField": "school_id",
            "foreignField": "school_id",
            "as": "school_data"
        }
    },
    {
        "$unwind": "$school_data"
    },
    {
        "$limit": 100
    }
]

result5 = mongo_db['zno_data_new'].aggregate(pipeline)
print("\nQuery 5 Result (Limited to 100 records):")
for record in result5:
    print(record)
