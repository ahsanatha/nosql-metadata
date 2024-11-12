# main.py
import os
from dotenv import load_dotenv
from scan import scan_mongo_database
from profiling import profile_mongo_database
from classification import detect_pii_fields, sample_data_and_detect_pii
import pandas as pd
from datetime import datetime
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()

# Retrieve all MongoDB connection strings from environment variables
mongo_uris = [os.getenv(f"MONGO_URI_{i}") for i in range(1, 201)]  # This will collect MONGO_URI_1 to MONGO_URI_200

# Initialize dictionaries to store results
all_profiling_data = {}
all_pii_data = {}

# Iterate through each MongoDB connection string
for mongo_uri in mongo_uris:
    if not mongo_uri:
        continue  # Skip if no connection string is provided
    
    # Create a MongoDB client for this connection string
    client = MongoClient(mongo_uri)
    
    # Specify the database name (use the first database in the URI for this example)
    db_name = mongo_uri.split('/')[1]  # This extracts the database name from the URI

    # Scan the MongoDB database to get the collection names
    collections = scan_mongo_database(db_name)

    # Profile the collections in the MongoDB database
    profiling_data = profile_mongo_database(client, db_name, collections)

    # Classify the PII data (fields and sample data)
    pii_data = []
    for collection_name in collections:
        collection_obj = client[db_name][collection_name]  # Get the collection object
        
        # Get the fields in the collection
        sample_document = collection_obj.find_one()
        fields = list(sample_document.keys()) if sample_document else ['No documents']
        
        # Classify PII fields based on field names
        pii_fields = detect_pii_fields(fields)
        
        # Classify PII data based on sampled data from the collection
        pii_sample_data = sample_data_and_detect_pii(collection_obj)
        
        # Append the PII results
        pii_data.append({
            'Collection': collection_name, 
            'PII Fields': pii_fields, 
            'Sample PII Data': pii_sample_data
        })
    
    # Store the profiling and PII data for each database
    all_profiling_data[db_name] = pd.DataFrame(profiling_data)
    all_pii_data[db_name] = pd.DataFrame(pii_data)

# Get the current timestamp and format it
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Set the app name and create the filename with timestamp
app_name = "MongoPIIDetector"
output_dir = '/result'
os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
output_file = os.path.join(output_dir, f"{app_name}_{timestamp}.xlsx")

# Save all profiling and PII data to separate sheets in an Excel file
with pd.ExcelWriter(output_file) as writer:
    for db_name, profiling_df in all_profiling_data.items():
        profiling_df.to_excel(writer, sheet_name=f'{db_name}_Profiling', index=False)
    
    for db_name, pii_df in all_pii_data.items():
        pii_df.to_excel(writer, sheet_name=f'{db_name}_PII', index=False)

print(f"Scan, profiling, and PII detection results saved to {output_file}")
