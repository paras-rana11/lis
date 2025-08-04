from datetime import datetime, timedelta
import json
import os
import logging

CASE_FILE = 'C:\\All\\LIS\\lis5 - remove old jsons\\'
FILE_NAME = 'new_case.json'
FULL_PATH = os.path.join(CASE_FILE, FILE_NAME)
os.makedirs(os.path.dirname(FULL_PATH), exist_ok=True)


# LOGGING SETUP
log_file = os.path.join(CASE_FILE, 'save_case_log.txt')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# SAVE CASE FUNCTION
def save_case_to_json(case_entry, machine_id, file_name):
    file_path = os.path.join(CASE_FILE, file_name)

    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []

        cutoff = datetime.now() - timedelta(days=10)

        updated = False
        for machine_dict in existing_data:
            if machine_id in machine_dict:
                
                machine_dict[machine_id] = [
                    entry for entry in machine_dict[machine_id]
                    if datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff
                ]
                
                if not any(entry["case_id"] == case_entry["case_id"] for entry in machine_dict[machine_id]):
                    machine_dict[machine_id].append(case_entry)
                    logging.info(f"✅ Case {case_entry['case_id']} added to machine {machine_id}")
                else:
                    logging.info(f"⚠️ Duplicate case {case_entry['case_id']} ignored for machine {machine_id}")
                updated = True
                break

        if not updated:
            existing_data.append({machine_id: [case_entry]})
            logging.info(f"✅ Case {case_entry['case_id']} added to new machine {machine_id}")

        with open(file_path, 'w') as f:
            json.dump(existing_data, f, indent=4)

    except Exception as e:
        logging.error(f"❌ Error saving case {case_entry.get('case_id', '?')}: {str(e)}")

if __name__ == "__main__":
    now = datetime.now()
    old = now - timedelta(days=11)

    test_cases = [
        {
            "machine_id": "ACCESS2",
            "case": {
                "case_id": "A001",
                "patient_id": "AP001",
                "test_id": "GLU,UREA",
                "sample_type": "Serum",
                "machine_id": "ACCESS2",
                "timestamp": old.strftime('%Y-%m-%d %H:%M:%S')
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

    for item in test_cases:
        save_case_to_json(item["case"], item["machine_id"], FILE_NAME)
