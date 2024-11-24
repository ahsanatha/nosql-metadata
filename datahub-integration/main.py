import requests
import sqlite3
import json
import time
import argparse
import pandas as pd
import re  # For regular expression matching

## How to use
# 1. Scan all the IP
# 2. Export Data

# there will be multiple IP to scan
IP = [
    "103.41.206.64",  # Add other IPs here
]

# Base URL for the API (will be updated dynamically for each IP)
base_url = "http://{IP}:8080/openapi/v3/entity/dataset"

# Parameters for the GET request
params = {
    'systemMetadata': 'false',
    'includeSoftDelete': 'false',
    'skipCache': 'false',
    'count': 10000,  # Default count of items per page
    'sort': 'urn',
    'sortOrder': 'ASCENDING',
    'query': '*',
}

# Column Patterns (Regular Expressions) for classification
# Define your patterns for each term here
column_patterns = {
    "NIK": r"(?i)^(.*)(_nik|nik|NIK|nomor[ _]?induk[ _]?kependudukan|citizen[ _]?id|identity[ _]?number)$",
    "No_SIM": r"(?i)^(.*)(_sim|sim|SIM|nomor[ _]?sim|surat[ _]?izin[ _]?mengemudi|sim[ _]?number|driving[ _]?license)$",
    "Email": r"(?i)^(.*)(_email|email|mail|email[ _]?address|email[ _]?id|user[ _]?email)$",
    "Jenis_Kelamin": r"(?i)^(.*)(_jenis[ _]?kelamin|gender|sex|sex[ _]?type|gender[ _]?type)$",
    "Nama_Lengkap": r"(?i)^(.*)(_nama[ _]?lengkap|fullname|full[ _]?name|name|person[ _]?name|user[ _]?name|complete[ _]?name)$",
    "Kewarganegaraan": r"(?i)^(.*)(_kewarganegaraan|wni|wna|nationality|citizenship|citizen)$",
    "Agama": r"(?i)^(.*)(_agama|religion|faith|belief|spirituality)$",
    "Status_Perkawinan": r"(?i)^(.*)(_status[ _]?perkawinan|marital[ _]?status|civil[ _]?status|marriage[ _]?status|marital[ _]?state)$",
    "NIK_Telkom": r"(?i)^(.*)(_nik[ _]?telkom|employee[ _]?id|employee[ _]?number|id[ _]?karyawan|telkom[ _]?employee[ _]?id)$",
    "Nomor_Layanan": r"(?i)^(.*)(_nomor[ _]?layanan|service[ _]?number|indihome|internet|service[ _]?id|service[ _]?no)$",
    "Paspor": r"(?i)^(.*)(_paspor|passport|passport[ _]?number|passport[ _]?id|passport[ _]?no)$",
    "NPWP": r"(?i)^(.*)(_npwp|taxpayer[ _]?id|tax[ _]?id|tax[ _]?number|npwp[ _]?number|tax[ _]?no)$",
    "Nomor_BPJS": r"(?i)^(.*)(_bpjs|bpjs[ _]?number|social[ _]?security[ _]?number|bpjs[ _]?id)$",
    "No_HP": r"(?i)^(.*)(_no[ _]?hp|phone[ _]?number|telephone[ _]?number|mobile[ _]?number|nomor[ _]?telepon|cell[ _]?phone)$",
    "Lokasi_GPS": r"(?i)^(.*)(_lokasi[ _]?gps|gps[ _]?location|geo[ _]?location|coordinates|gps[ _]?coordinates|location[ _]?gps)$",
    "Alamat": r"(?i)^(.*)(_alamat|address|address[ _]?line|location|street[ _]?address|residence[ _]?address|home[ _]?address)$",
    "Password": r"(?i)^(.*)(_password|pwd|passwd|user[ _]?password|authentication[ _]?key|secret[ _]?key)$",
    "Golongan_Darah": r"(?i)^(.*)(_golongan[ _]?darah|blood[ _]?type|blood[ _]?group|blood[ _]?type[ _]?group)$",
    "Nomor_Akta_Perkawinan": r"(?i)^(.*)(_nomor[ _]?akta[ _]?perkawinan|marriage[ _]?certificate[ _]?number|marriage[ _]?id|marriage[ _]?certificate)$",
    "Alamat_IP": r"(?i)^(.*)(_ip[ _]?address|ip[ _]?address|internet[ _]?protocol[ _]?address|ip[ _]?location)$",
    "ID_pengguna": r"(?i)^(.*)(_id[ _]?pengguna|user[ _]?id|user[ _]?identifier|account[ _]?id|account[ _]?number|user[ _]?identifier)$",
    "ID_Social_Media": r"(?i)^(.*)(_social[ _]?media[ _]?id|social[ _]?media[ _]?account|social[ _]?media[ _]?identifier|social[ _]?id|social[ _]?account)$",
    "Nama_Ibu_Kandung": r"(?i)^(.*)(_nama[ _]?ibu[ _]?kandung|mother[ _]?name|mom[ _]?name|birth[ _]?mother[ _]?name)$",
    "Suku_Ras": r"(?i)^(.*)(_suku|ethnicity|race|heritage|ethnic[ _]?group|ethnic[ _]?origin)$",
    "Nomor_Kartu_Keluarga_KK": r"(?i)^(.*)(_nomor[ _]?kk|kartu[ _]?keluarga|family[ _]?card[ _]?number|family[ _]?id)$",
    "PIN": r"(?i)^(.*)(_pin|personal[ _]?identification[ _]?number|security[ _]?code|security[ _]?pin)$",
    "Gaji": r"(?i)^(.*)(_gaji|salary|income|wage|compensation|pay[ _]?rate|salary[ _]?amount|monthly[ _]?salary)$",
    "Nomor_Kartu_Kredit": r"(?i)^(.*)(_kartu[ _]?kredit|credit[ _]?card|cc|credit[ _]?card[ _]?number|cvv|credit[ _]?card[ _]?id)$",
    "Informasi_medis_pribadi": r"(?i)^(.*)(_informasi[ _]?medis[ _]?pribadi|health[ _]?information|medical[ _]?records|personal[ _]?health[ _]?data|medical[ _]?info)$",
    "Riwayat_kesehatan": r"(?i)^(.*)(_riwayat[ _]?kesehatan|health[ _]?history|medical[ _]?history|past[ _]?medical[ _]?records)$",
    "Catatan_medis": r"(?i)^(.*)(_catatan[ _]?medis|medical[ _]?notes|health[ _]?records|medical[ _]?logs)$",
    "Hasil_tes_laboratorium": r"(?i)^(.*)(_tes[ _]?laboratorium|lab[ _]?results|laboratory[ _]?test[ _]?results|test[ _]?results|lab[ _]?test[ _]?outcome)$",
    "Diagnosis_penyakit": r"(?i)^(.*)(_diagnosis[ _]?penyakit|medical[ _]?diagnosis|disease[ _]?diagnosis|health[ _]?diagnosis)$",
    "Rincian_transaksi_keuangan": r"(?i)^(.*)(_transaksi[ _]?keuangan|financial[ _]?transaction[ _]?details|transaction[ _]?details|financial[ _]?details)$",
    "Perjanjian_pinjaman": r"(?i)^(.*)(_perjanjian[ _]?pinjaman|loan[ _]?agreement|loan[ _]?contract|loan[ _]?details)$",
    "Detail_pensiun": r"(?i)^(.*)(_pensiun|retirement[ _]?details|pension[ _]?details|retirement[ _]?info)$",
    "Laporan_keuangan": r"(?i)^(.*)(_laporan[ _]?keuangan|financial[ _]?report|financial[ _]?statements|financial[ _]?summary|financial[ _]?overview)$",
    "Laporan_kredit": r"(?i)^(.*)(_laporan[ _]?kredit|credit[ _]?report|credit[ _]?score[ _]?report|credit[ _]?statement)$",
    "Cookies": r"(?i)^(.*)(_cookies|browser[ _]?cookies|cookie[ _]?data|web[ _]?cookies)$",
}

db_type_pattern = r".*MongoDB.*"  # Regex pattern for db_type to match MongoDB, case-insensitive

# SQLite Database Setup
def setup_db():
    conn = sqlite3.connect('datahub_metadata.db')
    cursor = conn.cursor()

    # Create tables to store structured data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS databases (
        id INTEGER PRIMARY KEY,
        db_name TEXT,
        db_type TEXT,
        ip TEXT
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
def fetch_datasets(ip, params):
    """Fetch datasets from the API with the given parameters."""
    url = base_url.format(IP=ip)  # Update the base URL with the current IP
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data from {ip}: {response.status_code}")
        return None

def save_to_db(all_datasets, ip, ensure_unique=True):
    """Save the fetched datasets to the SQLite database. 
    If ensure_unique is True, it ensures uniqueness by db_name, db_type, and column_name.
    """
    conn = setup_db()
    cursor = conn.cursor()

    # Dictionary to temporarily store unique data (only used if ensure_unique is True)
    if ensure_unique:
        unique_data = {}

    for dataset in all_datasets:
        urn = dataset.get('urn')

        # Database info extraction
        db_name = dataset.get('datasetKey', {}).get('value', {}).get('name')
        db_type = dataset.get('datasetKey', {}).get('value', {}).get('platform')

        # Insert database info into the unique_data dictionary, keyed by (db_name, db_type, column_name)
        schema_metadata = dataset.get('schemaMetadata', {}).get('value', {})
        if 'fields' in schema_metadata:
            for field in schema_metadata['fields']:
                column_name = field.get('fieldPath')
                native_data_type = field.get('nativeDataType')
                nullable = field.get('nullable', False)

                # Logic for ensuring uniqueness (if applicable)
                if ensure_unique:
                    key = (db_name, db_type, column_name)
                    if key not in unique_data:
                        # Add unique entry to the dictionary
                        unique_data[key] = {
                            'db_name': db_name,
                            'db_type': db_type,
                            'column_name': column_name,
                            'native_data_type': native_data_type,
                            'nullable': nullable,
                            'terms': set()  # Use a set to store terms for uniqueness
                        }

                    # Classify columns based on defined patterns
                    for term, pattern in column_patterns.items():
                        if re.match(db_type_pattern, db_type, re.IGNORECASE):
                            if re.match(pattern, column_name):
                                # If column name matches the pattern, classify it with the term
                                classification_term = f"{term} Column"
                                unique_data[key]['terms'].add(classification_term)  # Add term to the set
                        else:
                            if 'glossaryTerms' in field:
                                for term in field['glossaryTerms'].get('terms', []):
                                    glossary_term = term.get('urn', None)
                                    if glossary_term:
                                        unique_data[key]['terms'].add(glossary_term)  # Add glossary term to the set
                else:
                    # Directly classify and insert data without uniqueness check
                    key = (db_name, db_type, column_name)
                    cursor.execute('INSERT OR IGNORE INTO databases (db_name, db_type, ip) VALUES (?, ?, ?)', 
                                   (db_name, db_type, ip))
                    db_id = cursor.lastrowid

                    # Insert column info into columns table
                    cursor.execute('INSERT INTO columns (db_id, column_name, native_data_type, nullable) VALUES (?, ?, ?, ?)', 
                                   (db_id, column_name, native_data_type, nullable))
                    column_id = cursor.lastrowid

                    # Classify columns based on defined patterns
                    for term, pattern in column_patterns.items():
                        if re.match(db_type_pattern, db_type, re.IGNORECASE):
                            if re.match(pattern, column_name):
                                classification_term = f"{term} Column"
                                cursor.execute('INSERT INTO glossary_terms (column_id, term) VALUES (?, ?)', 
                                               (column_id, classification_term))
                        else:
                            if 'glossaryTerms' in field:
                                for term in field['glossaryTerms'].get('terms', []):
                                    glossary_term = term.get('urn', None)
                                    if glossary_term:
                                        cursor.execute('INSERT INTO glossary_terms (column_id, term) VALUES (?, ?)', 
                                                       (column_id, glossary_term))

    if ensure_unique:
        # Insert unique data into the database
        for key, value in unique_data.items():
            db_name = value['db_name']
            db_type = value['db_type']
            column_name = value['column_name']
            native_data_type = value['native_data_type']
            nullable = value['nullable']

            # Insert database info into databases table, including IP address
            cursor.execute('INSERT OR IGNORE INTO databases (db_name, db_type, ip) VALUES (?, ?, ?)', (db_name, db_type, ip))
            db_id = cursor.lastrowid

            # Insert column info into columns table
            cursor.execute('INSERT OR IGNORE INTO columns (db_id, column_name, native_data_type, nullable) VALUES (?, ?, ?, ?)', 
                           (db_id, column_name, native_data_type, nullable))
            column_id = cursor.lastrowid

            # Insert glossary terms into glossary_terms table
            for term in value['terms']:
                cursor.execute('INSERT INTO glossary_terms (column_id, term) VALUES (?, ?)', 
                               (column_id, term))

    conn.commit()
    conn.close()


def fetch_all_datasets(ip, max_limit=10000, count=1000, unique=True):
    """Fetch all datasets using scrollId for pagination."""
    all_datasets = []
    total_fetched = 0
    params['count'] = count  # Update the count dynamically based on CLI input

    data = fetch_datasets(ip, params)

    if data and 'entities' in data:
        all_datasets.extend(data['entities'])
        total_fetched += len(data['entities'])
        print(f"Fetched {len(data['entities'])} datasets from {ip} (Total: {total_fetched})")
        save_to_db(all_datasets, ip, unique)  # Save to DB after the first batch

        scroll_id = data.get('scrollId', None)

        while scroll_id and total_fetched < max_limit:
            params['scrollId'] = scroll_id
            time.sleep(2)
            data = fetch_datasets(ip, params)

            if data and 'entities' in data:
                all_datasets.extend(data['entities'])
                total_fetched += len(data['entities'])
                print(f"Fetched {len(data['entities'])} datasets from {ip} (Total: {total_fetched})")
                save_to_db(all_datasets, ip, unique)

                scroll_id = data.get('scrollId', None)

                if total_fetched % 1000 == 0:
                    print(f"Progress: {total_fetched} datasets fetched from {ip}...")
            else:
                break

    return all_datasets

def export_to_json(target_file):
    """Export data from SQLite to JSON."""
    conn = sqlite3.connect('datahub_metadata.db')
    cursor = conn.cursor()

    # Fetch databases and their columns, glossary terms
    cursor.execute('''
    SELECT db_name, db_type, column_name, native_data_type, nullable, term, ip 
    FROM databases
    JOIN columns ON databases.id = columns.db_id
    LEFT JOIN glossary_terms ON columns.id = glossary_terms.column_id
    ''')

    data = cursor.fetchall()

    # Structure the data
    result = []
    for row in data:
        db_name, db_type, column_name, native_data_type, nullable, term, ip = row
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
            'glossary_term': term,
            'ip': ip
        })

    # Save to JSON
    with open(target_file, 'w') as f:
        json.dump(result, f, indent=4)

    print(f"Data exported to {target_file}!")

def export_to_excel(target_file, row_limit=50000):
    """Export data from SQLite to Excel. If the file is too large, split it into multiple files."""
    conn = sqlite3.connect('datahub_metadata.db')
    cursor = conn.cursor()

    # Fetch data for exporting to Excel
    cursor.execute('''
    SELECT db_name, db_type, column_name, native_data_type, nullable, term, ip 
    FROM databases
    JOIN columns ON databases.id = columns.db_id
    LEFT JOIN glossary_terms ON columns.id = glossary_terms.column_id
    ''')

    data = cursor.fetchall()

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=['db_name', 'db_type', 'column_name', 'native_data_type', 'nullable', 'term', 'ip'])

    # Check if the number of rows exceeds the limit
    num_rows = len(df)
    if num_rows > row_limit:
        print(f"Data has {num_rows} rows, which exceeds the row limit of {row_limit}. Splitting into multiple files...")
        
        # Calculate how many parts we need
        num_parts = (num_rows // row_limit) + 1
        
        for i in range(num_parts):
            # Split the DataFrame into smaller parts
            start_row = i * row_limit
            end_row = (i + 1) * row_limit
            df_chunk = df.iloc[start_row:end_row]
            
            # Save each chunk into a separate Excel file
            file_name = f"{target_file.split('.xlsx')[0]}_{i + 1}.xlsx"
            df_chunk.to_excel(file_name, index=False, engine='openpyxl')
            print(f"Data exported to {file_name}!")
    else:
        # If the number of rows does not exceed the limit, export the data to one file
        df.to_excel(target_file, index=False, engine='openpyxl')
        print(f"Data exported to {target_file}!")


def main():
    parser = argparse.ArgumentParser(description='DataHub Metadata Processing')
    
    # Adding arguments for CLI commands
    parser.add_argument('--scan', action='store_true', help='Scan data and save to SQLite')
    parser.add_argument('--export', choices=['json', 'excel'], help='Export data to json or excel')
    parser.add_argument('--count', type=int, default=1000, help='Number of items per page (default: 1000)')
    parser.add_argument('--file', type=str, help='Specify the target file for exporting (json or excel)')
    parser.add_argument('--unique', action='store_true', default=False, help='Ensure uniqueness by db_name, db_type, and column_name (default: False)')

    args = parser.parse_args()

    if args.scan:
        for ip in IP:
            print(f"Scanning data from {ip} and saving to SQLite with {args.count} items per page...")
            datasets = fetch_all_datasets(ip, max_limit=10000, count=args.count, unique=args.unique)
            print(f"Scan complete for {ip}.")

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
