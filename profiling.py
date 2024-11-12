# profiling.py
import re
from bson import ObjectId
import random

# PII Field Name Regex to detect PII-related field names
PII_FIELD_REGEX = r"(email|telepon|no_hp|ssn|nama|dob|alamat|ktp|identity|no_ktp|tanggal_lahir|nama_lengkap|alamat_rumah|contact)"

# PII Data Regex patterns for various PII data types (values)
PII_DATA_REGEX = {
    'email': r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",  # Email pattern
    'phone': r"^(?:\+62|62|0)8\d{8,11}$",  # Indonesian phone number pattern (starts with +62, 62, or 08)
    'ssn': r"^\d{6,12}$",  # Indonesian ID card (KTP) number pattern, length between 6-12 digits
    'name': r"^[A-Za-z\s]+$",  # Name pattern (only letters and spaces, as in common Indonesian names)
    'dob': r"^\d{4}-\d{2}-\d{2}$",  # Date of birth (YYYY-MM-DD format)
    'address': r"^\d{1,5}\s[\w\s]+(?:RT|RW)?[\w\s]+$",  # Simple address regex (matches street names with RT/RW, common in Indonesia)
    'identity': r"^\d{16}$",  # Indonesian National ID (KTP) number, 16 digits
}

# Mapping MongoDB types to pymongo type codes
MONGO_TYPE_CODES = {
    str: 2,  # string
    int: 16,  # integer
    bool: 18,  # boolean
    float: 10,  # float
    ObjectId: 7,  # ObjectId
    list: 8,  # array
    dict: 4,  # object
}

def profile_mongo_database(client, db_name, collections):
    """
    Profile the MongoDB collections and compute data quality metrics.
    :param client: The MongoClient object to connect to MongoDB.
    :param db_name: The name of the MongoDB database to profile.
    :param collections: A list of collection names in the database.
    :return: A list of profiling data for each collection.
    """
    data = []
    db = client[db_name]
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Get fields in the collection (using the first document in the collection)
        sample_document = collection.find_one()
        fields = list(sample_document.keys()) if sample_document else ['No documents']
        
        # List all indexes in the collection
        indexes = collection.index_information()
        
        # Get collection statistics using aggregation pipeline with $collStats
        pipeline = [{"$collStats": {"storageStats": {}}}]
        stats = list(collection.aggregate(pipeline))
        
        completeness = check_completeness(collection)
        consistency = check_consistency(collection)
        uniqueness = check_uniqueness(collection, fields)
        timeliness = check_timeliness(collection)
        validity = check_validity(collection, fields)

        if stats:
            stats = stats[0]
            
            data.append({
                'Collection': collection_name,
                'Fields': ', '.join(fields),
                'Index Name': ', '.join([index_name for index_name in indexes]),
                'Data Size (bytes)': stats.get('storageStats', {}).get('dataSize', 'N/A'),
                'File Size (bytes)': stats.get('storageStats', {}).get('storageSize', 'N/A'),
                'Num Objects': stats.get('storageStats', {}).get('numObjects', 'N/A'),
                'Avg Object Size (bytes)': stats.get('storageStats', {}).get('avgObjSize', 'N/A'),
                'Total Index Size (bytes)': stats.get('storageStats', {}).get('totalIndexSize', 'N/A'),
                'Completeness': completeness,
                'Consistency': consistency,
                'Uniqueness': uniqueness,
                'Timeliness': timeliness,
                'Validity': validity,
            })

    return data

# Data Quality Check Functions
def check_completeness(collection):
    required_fields = ['name', 'email']
    missing_count = 0
    total_count = collection.count_documents({})
    for field in required_fields:
        missing_count += collection.count_documents({field: {'$exists': False}})
    return (total_count - missing_count) / total_count * 100 if total_count else 100

def check_consistency(collection):
    inconsistent_count = 0
    sample_document = collection.find_one()
    if sample_document:
        for key, value in sample_document.items():
            field_type_code = MONGO_TYPE_CODES.get(type(value), None)
            if field_type_code is None:
                continue
            inconsistent_count += collection.count_documents({key: {'$not': {'$type': field_type_code}}})
    consistency = 100 - (inconsistent_count / collection.count_documents({})) * 100 if collection.count_documents({}) else 100
    return f"{consistency:.2f}%"

def check_uniqueness(collection, fields):
    unique_count = collection.count_documents({})
    duplicate_count = collection.aggregate([
        {'$group': {'_id': {key: f"${key}" for key in fields}, 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gt': 1}}}
    ])
    duplicate_count = sum(1 for _ in duplicate_count)
    uniqueness = 100 - (duplicate_count / unique_count * 100) if unique_count else 100
    return f"{uniqueness:.2f}%"

def check_timeliness(collection):
    latest_document = collection.find_one(sort=[('updated_at', -1)])
    if latest_document:
        last_updated = latest_document.get('updated_at')
        timeliness = "Up-to-date" if last_updated else "Unknown"
    else:
        timeliness = "No documents"
    return timeliness

def check_validity(collection, fields):
    validity_count = 0
    for field in fields:
        if field == 'email':
            validity_count += collection.count_documents({field: {'$not': {'$regex': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}}})
    return 100 - (validity_count / collection.count_documents({})) * 100 if collection.count_documents({}) else 100
