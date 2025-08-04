from datetime import datetime, timedelta
import json
import os

# Configuration
CASE_FILE = 'C:\\All\\LIS\\lis5 - remove old jsons\\simple_format'
FILE_NAME = 'simple_case.json'
os.makedirs(os.path.dirname(CASE_FILE), exist_ok=True)

def delete_old_cases(file_path, days=10):
    try:
        if not os.path.exists(file_path):
            print("---- File not found.")
            return

        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print("---- JSON decode error.")
                return

        cutoff_date = datetime.now() - timedelta(days=days)

        modified = False
        for machine_id in data:
            original_len = len(data[machine_id])
            data[machine_id] = [
                entry for entry in data[machine_id]
                if datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff_date
            ]
            if len(data[machine_id]) != original_len:
                modified = True
                print(f"-- Removed old cases for machine: {machine_id}")

        if modified:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
            print("-- Old cases deleted.")
        else:
            print("---- No old cases found.")

    except Exception as e:
        print(f"---- Error deleting old cases: {e}")
        
        
        
        
delete_old_cases(os.path.join(CASE_FILE, FILE_NAME))

