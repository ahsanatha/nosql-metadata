# classification.py
import re
import random

# PII Field Name Regex to detect PII-related field names
PII_FIELD_REGEX = r"(email|telepon|no_hp|ssn|nama|dob|alamat|ktp|identity|no_ktp|tanggal_lahir|nama_lengkap|alamat_rumah|contact)"
PII_DATA_REGEX = {
    'email': r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    'phone': r"^(?:\+62|62|0)8\d{8,11}$",
    'ssn': r"^\d{6,12}$",
    'name': r"^[A-Za-z\s]+$",
    'dob': r"^\d{4}-\d{2}-\d{2}$",
    'address': r"^\d{1,5}\s[\w\s]+(?:RT|RW)?[\w\s]+$",
    'identity': r"^\d{16}$",
}

def detect_pii_fields(fields):
    pii_fields = [field for field in fields if re.search(PII_FIELD_REGEX, field, re.IGNORECASE)]
    return pii_fields

def sample_data_and_detect_pii(collection, sample_size=10):
    sampled_documents = random.sample(list(collection.find()), min(sample_size, collection.count_documents({})))
    pii_data_found = []
    
    for doc in sampled_documents:
        for field, value in doc.items():
            if isinstance(value, str):  # Only check if the value is a string
                for pii_type, pii_regex in PII_DATA_REGEX.items():
                    if re.match(pii_regex, value):
                        pii_data_found.append({'field': field, 'value': value, 'type': pii_type})
    
    return pii_data_found
