from datetime import datetime, timedelta
import json
import os

# Configuration
CASE_FILE = 'C:\\All\\LIS\\lis5 - remove old jsons\\'
FILE_NAME = 'case.json'
os.makedirs(os.path.dirname(CASE_FILE), exist_ok=True)

def save_case_to_json(case_entry, machine_id, file_name):
    file_path = os.path.join(CASE_FILE, file_name)

    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as json_file:
                try:
                    existing_data = json.load(json_file)
                except json.JSONDecodeError:
                    existing_data = [[]]
        else:
            existing_data = [[]]

        cutoff_date = datetime.now() - timedelta(days=10)

        for machine_group in existing_data:
            for machine in machine_group:
                if machine_id in machine:
                    machine[machine_id] = [
                        entry for entry in machine[machine_id]
                        if datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff_date
                    ]

        machine_found = False
        for machine_group in existing_data:
            for machine in machine_group:
                if machine_id in machine:
                    if not any(entry["case_id"] == case_entry["case_id"] for entry in machine[machine_id]):
                        machine[machine_id].append(case_entry)
                    machine_found = True
                    break
            if machine_found:
                break

        if not machine_found:
            existing_data[0].append({machine_id: [case_entry]})

        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)

        print(f"✅ Case saved for {machine_id}: {case_entry['case_id']}")

    except Exception as e:
        print(f"❌ Error: {e}")

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


