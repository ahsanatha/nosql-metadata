# scan.py
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URI from environment variable
mongo_uri = os.getenv("MONGO_URI")

# Connect to MongoDB using the connection string from the .env file
client = MongoClient(mongo_uri)

def scan_mongo_database(db_name):
    """
    Scan the MongoDB database and get collections.
    :param db_name: The name of the MongoDB database to scan.
    :return: A list of collections in the database.
    """
    # Connect to the database
    db = client[db_name]
    
    # List all collections in the specified database
    collections = db.list_collection_names()
    
    return collections
