import requests
import sqlite3
import json
import time
import argparse
import pandas as pd

# Base URL for the API
base_url = "http://hostaddr:8080/openapi/v3/entity/dataset"

# Parameters for the GET request
params = {
    'systemMetadata': 'false',
    'includeSoftDelete': 'false',
    'skipCache': 'false',
    'count': 1000,  # Default count of items per page
    'sort': 'urn',
    'sortOrder': 'ASCENDING',
    'query': '*',
}

# SQLite Database Setup
def setup_db():
    conn = sqlite3.connect('datahub_metadata.db')
    cursor = conn.cursor()

    # Create tables to store the raw data and structured data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_data (
        id INTEGER PRIMARY KEY,
        urn TEXT,
        raw_json TEXT
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS databases (
        id INTEGER PRIMARY KEY,
        db_name TEXT,
        db_type TEXT
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS columns (
        id INTEGER PRIMARY KEY,
        db_id INTEGER,
        column_name TEXT,
        native_data_type TEXT,
        nullable BOOLEAN,
        FOREIGN KEY(db_id) REFERENCES databases(id)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS glossary_terms (
        id INTEGER PRIMARY KEY,
        column_id INTEGER,
        term TEXT,
        FOREIGN KEY(column_id) REFERENCES columns(id)
    )''')

    conn.commit()
    return conn

# Fetch the datasets
def fetch_datasets(params):
    """Fetch datasets from the API with the given parameters."""
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def save_to_db(all_datasets):
    """Save the fetched datasets to the SQLite database."""
    conn = setup_db()
    cursor = conn.cursor()

    for dataset in all_datasets:
        urn = dataset.get('urn')
        raw_json = json.dumps(dataset)  # Store raw JSON

        # Insert raw data into the raw_data table
        cursor.execute('INSERT INTO raw_data (urn, raw_json) VALUES (?, ?)', (urn, raw_json))
        raw_data_id = cursor.lastrowid

        # Database info extraction
        db_name = dataset.get('datasetKey', {}).get('value', {}).get('name')
        db_type = dataset.get('datasetKey', {}).get('value', {}).get('platform')

        # Insert database info into databases table
        cursor.execute('INSERT OR IGNORE INTO databases (db_name, db_type) VALUES (?, ?)', (db_name, db_type))
        db_id = cursor.lastrowid

        # Insert columns and glossary terms
        schema_metadata = dataset.get('schemaMetadata', {}).get('value', {})
        if 'fields' in schema_metadata:
            for field in schema_metadata['fields']:
                column_name = field.get('fieldPath')
                native_data_type = field.get('nativeDataType')
                nullable = field.get('nullable', False)

                # Insert column info into columns table
                cursor.execute('INSERT INTO columns (db_id, column_name, native_data_type, nullable) VALUES (?, ?, ?, ?)', 
                               (db_id, column_name, native_data_type, nullable))
                column_id = cursor.lastrowid

                # Insert glossary terms into glossary_terms table
                if 'glossaryTerms' in field:
                    for term in field['glossaryTerms'].get('terms', []):
                        glossary_term = term.get('urn', None)
                        if glossary_term:
                            cursor.execute('INSERT INTO glossary_terms (column_id, term) VALUES (?, ?)', 
                                           (column_id, glossary_term))

    conn.commit()
    conn.close()

def fetch_all_datasets(max_limit=10000, count=1000):
    """Fetch all datasets using scrollId for pagination."""
    all_datasets = []
    total_fetched = 0
    params['count'] = count  # Update the count dynamically based on CLI input

    data = fetch_datasets(params)

    if data and 'entities' in data:
        all_datasets.extend(data['entities'])
        total_fetched += len(data['entities'])
        print(f"Fetched {len(data['entities'])} datasets (Total: {total_fetched})")
        save_to_db(all_datasets)  # Save to DB after the first batch

        scroll_id = data.get('scrollId', None)

        while scroll_id and total_fetched < max_limit:
            params['scrollId'] = scroll_id
            time.sleep(2)
            data = fetch_datasets(params)

            if data and 'entities' in data:
                all_datasets.extend(data['entities'])
                total_fetched += len(data['entities'])
                print(f"Fetched {len(data['entities'])} datasets (Total: {total_fetched})")
                save_to_db(all_datasets)

                scroll_id = data.get('scrollId', None)

                if total_fetched % 1000 == 0:
                    print(f"Progress: {total_fetched} datasets fetched...")
            else:
                break

    return all_datasets

def export_to_json(target_file):
    """Export data from SQLite to JSON."""
    conn = sqlite3.connect('datahub_metadata.db')
    cursor = conn.cursor()

    # Fetch databases and their columns, glossary terms
    cursor.execute('''
    SELECT db_name, db_type, column_name, native_data_type, nullable, term 
    FROM databases
    JOIN columns ON databases.id = columns.db_id
    LEFT JOIN glossary_terms ON columns.id = glossary_terms.column_id
    ''')

    data = cursor.fetchall()

    # Structure the data
    result = []
    for row in data:
        db_name, db_type, column_name, native_data_type, nullable, term = row
        db = next((item for item in result if item['db_name'] == db_name), None)
        if db is None:
            db = {
                'db_name': db_name,
                'db_type': db_type,
                'columns': []
            }
            result.append(db)
        db['columns'].append({
            'column_name': column_name,
            'native_data_type': native_data_type,
            'nullable': nullable,
            'glossary_term': term
        })

    # Save to JSON
    with open(target_file, 'w') as f:
        json.dump(result, f, indent=4)

    print(f"Data exported to {target_file}!")

def export_to_excel(target_file):
    """Export data from SQLite to Excel."""
    conn = sqlite3.connect('datahub_metadata.db')
    cursor = conn.cursor()

    # Fetch data for exporting to Excel
    cursor.execute('''
    SELECT db_name, db_type, column_name, native_data_type, nullable, term 
    FROM databases
    JOIN columns ON databases.id = columns.db_id
    LEFT JOIN glossary_terms ON columns.id = glossary_terms.column_id
    ''')

    data = cursor.fetchall()

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=['db_name', 'db_type', 'column_name', 'native_data_type', 'nullable', 'term'])

    # Save DataFrame to Excel
    df.to_excel(target_file, index=False, engine='openpyxl')
    print(f"Data exported to {target_file}!")

def main():
    parser = argparse.ArgumentParser(description='DataHub Metadata Processing')
    
    # Adding arguments for CLI commands
    parser.add_argument('--scan', action='store_true', help='Scan data and save to SQLite')
    parser.add_argument('--export', choices=['json', 'excel'], help='Export data to json or excel')
    parser.add_argument('--count', type=int, default=1000, help='Number of items per page (default: 1000)')
    parser.add_argument('--file', type=str, help='Specify the target file for exporting (json or excel)')

    args = parser.parse_args()

    if args.scan:
        print(f"Scanning data and saving to SQLite with {args.count} items per page...")
        datasets = fetch_all_datasets(max_limit=10000, count=args.count)
        print("Scan complete.")

    elif args.export == 'json':
        if not args.file:
            print("Please specify the target file using --file.")
        else:
            print(f"Exporting data to JSON at {args.file}...")
            export_to_json(args.file)

    elif args.export == 'excel':
        if not args.file:
            print("Please specify the target file using --file.")
        else:
            print(f"Exporting data to Excel at {args.file}...")
            export_to_excel(args.file)

if __name__ == '__main__':
    main()
