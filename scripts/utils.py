import pymongo
from pymongo.errors import ConnectionFailure

def connect_to_mongodb(mongo_uri, mongo_db_name, timeout = 5000):
    """Create a MongoDB client and connection."""
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=timeout)

        # Test connection
        client.admin.command('ismaster')
        client.admin.command('ping')

        databases = client.list_database_names()
        print(f"Databases: {databases}")
        
        # Get or create database
        database = client[mongo_db_name]

        print(f"MongoDB connection established to database: {mongo_db_name}")

        return client, database
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None, None
    