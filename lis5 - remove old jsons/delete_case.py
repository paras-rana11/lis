from datetime import datetime, timedelta
import json
import os

# Configuration
CASE_FILE = 'C:\\All\\LIS\\lis5 - remove old jsons\\'
FILE_NAME = 'case.json'
os.makedirs(CASE_FILE, exist_ok=True)

def delete_old_cases_only(file_name):
    file_path = os.path.join(CASE_FILE, file_name)

    try:
        if not os.path.exists(file_path):
            print("--- JSON file does not exist.")
            return

        with open(file_path, 'r') as json_file:
            try:
                existing_data = json.load(json_file)
            except json.JSONDecodeError:
                print("--- JSON Decode Error.")
                return

        cutoff_date = datetime.now() - timedelta(days=10)
        total_deleted = 0

        for machine_group in existing_data:
            for machine in machine_group:
                for machine_id in machine:
                    machine[machine_id] = [
                        entry for entry in machine[machine_id]
                        if datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff_date
                    ]

        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)


    except Exception as e:
        print(f"--- Error: {e}")

# 🔃 Run the cleaning function
delete_old_cases_only(FILE_NAME)
