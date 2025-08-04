from datetime import datetime, timedelta
import json
import os

# Configuration
CASE_FILE = 'C:\\All\\LIS\\lis5 - remove old jsons\\simple_format'
FILE_NAME = 'simple_case.json'
os.makedirs(os.path.dirname(CASE_FILE), exist_ok=True)

def save_case_to_json(case_entry, machine_id, file_name):
    file_path = os.path.join(CASE_FILE, file_name)

    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as json_file:
                try:
                    existing_data = json.load(json_file)
                except json.JSONDecodeError:
                    existing_data = {}
        else:
            existing_data = {}

        cutoff_date = datetime.now() - timedelta(days=10)

        if machine_id in existing_data:
            existing_data[machine_id] = [
                entry for entry in existing_data[machine_id]
                if datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff_date
            ]
        else:
            existing_data[machine_id] = []
            
        duplicate_found = False
        for entry in existing_data[machine_id]:
            if entry["case_id"] == case_entry["case_id"]:
                duplicate_found = True
                break

        if not duplicate_found:
            existing_data[machine_id].append(case_entry)


        # if not any(entry["case_id"] == case_entry["case_id"] for entry in existing_data[machine_id]):
        #     existing_data[machine_id].append(case_entry)

        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)

        print(f"- Saved: {case_entry['case_id']} to {machine_id}")

    except Exception as e:
        print(f"-- Error saving case: {e}")


# -------------------------------
# 🚀 DUMMY DATA FOR TESTING
# -------------------------------

now = datetime.now()
old_date = now - timedelta(days=11)

cases = [
    {
        "machine_id": "ACCESS2",
        "case": {
            "case_id": "A001",
            "patient_id": "AP001",
            "test_id": "GLU,UREA",
            "sample_type": "Serum",
            "machine_id": "ACCESS2",
            "timestamp": old_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    },
    {
        "machine_id": "COBAS311",
        "case": {
            "case_id": "C001",
            "patient_id": "CP001",
            "test_id": "Na,K",
            "sample_type": "Plasma",
            "machine_id": "COBAS311",
            "timestamp": now.strftime('%Y-%m-%d %H:%M:%S')
        }
    }
]

# ⏺️ SAVE DUMMY CASES
for c in cases:
    save_case_to_json(c["case"], c["machine_id"], FILE_NAME)


