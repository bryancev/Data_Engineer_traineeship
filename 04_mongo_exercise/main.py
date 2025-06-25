from pymongo import MongoClient

# MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["test_db"]
mongo_collection = mongo_db["users"]
mongo_collection.insert_one({"name": "Charlie"})
print("MongoDB:", list(mongo_collection.find({}, {"_id": 0})))