import json
import logging
import os
import re
import time
import uuid
import hashlib
import shutil
import mysql.connector
from mysql.connector import Error


# Configuration
my_host = '127.0.0.1'
my_user = 'root'
my_pass = 'root1'
my_db = 'lis'
folder_path = 'C:\\Users\\kaushik\\Desktop\\lis\\report_file'
backup_folder_path = 'C:\\Users\\kaushik\\Desktop\\lis\\backup_file'

def read_file(file_path):
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except Exception as e:
        print("Error in File Reading: ", e)
        return False
 
def get_connection():
    try:
        con = mysql.connector.connect(
            host = my_host,
            user = my_user,
            password = my_pass,
            database = my_db,
        )
        
        if con.is_connected:
            return con
        else:
            print("Connection Not succeed.")
    except mysql.connector.Error as e:
        print("MySQL Error: ", e)
        return False
    
def close_connection(con):
    try:
        if con:
            con.close()
    except mysql.connector.Error as e:
        print("MySQL Error: ", e)
        return False

def get_cursor(con):
    try:
        if con:
            return con.cursor() 
    except mysql.connector.Error as e:
        print("MySQL Error: ", e)
        return False

def close_cursor(cur):
    try:
        if cur:
            cur.close() 
    except mysql.connector.Error as e:
        print("MySQL Error: ", e)
        return False


def send_data_to_mysql_simple(data_list): 
    con = get_connection()
    if not con:
        print("Failed to connect to database")
        return False

    try:
        cur = con.cursor()

        for data in data_list:
            machinename, patientid, test = data

            try:
                new_test = json.loads(test.replace("'", '"'))
            except Exception as e:
                print(f"Invalid JSON for patient '{patientid}':", test)
                continue  
         
            cur.execute("""
                SELECT test FROM MACHINE_DATA 
                WHERE machinename = %s AND patientid = %s
            """, (machinename, patientid))

            rows = cur.fetchall()

            is_exact_duplicate = False
            is_partial_merge = False
            merged_test_json = None
            
            for row in rows:
                existing_json_str = row[0]
                # print("\n=>row", row, "|| row[0]:", row[0])
                try:
                    old_test = json.loads(existing_json_str)
                    print("\n\nold_test:", old_test)
                    print("new_test:", new_test)
                    
                    if old_test == new_test:
                        print("\nmatched")
                        is_exact_duplicate = True
                        print(f"\n-- old duplicate row found for patient '{patientid}': {old_test}, skipping insert.")
                        break
                    
                    print("\nnot matched")
                    conflict = False
                    
                    for key in new_test:
                        if key in old_test and new_test[key] != old_test[key]:
                            print(f"⚠ Conflict for '{key}': old='{old_test[key]}', new='{new_test[key]}'")
                            conflict = True
                            break
                                      
                    print("-> conflict:", conflict)
                    if not conflict:
                        merged = old_test.copy()
                        merged.update(new_test)
                        is_partial_merge = True  
                        merged_test_json = json.dumps(merged, separators=(',', ':'))
                        print(f"-- Merging data for patient '{patientid}': {merged}")
                        break   

                        

                except Exception as e:
                    print(f"!! Error parsing existing DB JSON for patient '{patientid}': {e}")
                    continue               
            
            if is_exact_duplicate:
                continue

            elif is_partial_merge:
                cur.execute("""
                    UPDATE MACHINE_DATA 
                    SET test = %s 
                    WHERE machinename = %s AND patientid = %s
                    LIMIT 1
                """, (merged_test_json, machinename, patientid))
                print(f"-- Database updated with merged data for patient '{patientid}'")

            else:
                normalized_json = json.dumps(new_test, separators=(',', ':'))
                cur.execute("""
                    INSERT INTO MACHINE_DATA (machinename, patientid, test) 
                    VALUES (%s, %s, %s)
                """, (machinename, patientid, normalized_json))
                print(f"-- New test record inserted for patient '{patientid}'")

            
            
        con.commit()
        cur.close()

    except Exception as e:
        print("---- Error during DB operation:", e)
        return False

    finally:
        con.close()

    return True


# =======================================================================================================================================================================================#




def extract_data_from_access_2(data):
    MACHINE_NAME = 'ACCESS_2'
    patients = []
    unique_patient_ids = set()
    patient_id = None
    patient_tests = {}

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')
    
    lines = data.strip().split('\n')

    for line in lines:
        segment = line.strip().split('|')
        if line.startswith("2P"):
            patient_id = segment[2].strip()
            if patient_id not in unique_patient_ids:
                unique_patient_ids.add(patient_id)
                patient_tests[patient_id] = {}

        elif line.startswith("4R") :
            try:
                test_code_full = segment[2].strip()
                test_name = test_code_full.split("^")[3]
                test_value = segment[3].strip()
                patient_tests[patient_id][test_name] = test_value
            except IndexError:
                continue

    for pid, tests in patient_tests.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_atul(data):
    MACHINE_NAME = 'ATUL'
    patients = []
    patient_tests_dict = {}  
    patient_id = None

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')
        
    lines = data.strip().split('\n')

    for line in lines:
        line = line.strip()
        parts = line.split('|')

        if line.startswith('P|'):
            patient_id = parts[3].strip()
            if  patient_id not in patient_tests_dict:
                patient_tests_dict[patient_id] = {}

        elif line.startswith('R|') :
            try:
                test_name = parts[2].strip()
                test_value = parts[8].strip()
                if test_name and test_value:
                    patient_tests_dict[patient_id][test_name] = test_value
            except IndexError as e:
                print(" IndexError in R line:", e)
                continue

        for pid, tests in patient_tests_dict.items():
                patients.append((MACHINE_NAME, pid, str(tests)))
    

    return patients

def extract_data_from_au_480(data):
    MACHINE_NAME = 'AU_480'
    patients = []
    patient_dict = {}

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    data_blocks = data.strip().split("D ")

    for block in data_blocks:
        lines = block.strip().split()

        if len(lines) < 3:
            continue

        patient_id = lines[2].strip()

        if patient_id not in patient_dict:
            patient_dict[patient_id] = {}

        for i in range(4, len(lines), 2):
            test_name = lines[i].strip()
            test_value = lines[i+1].strip().replace('r', '')  
            patient_dict[patient_id][test_name] = test_value

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_c_111(data):
    MACHINE_NAME = 'C_111'
    patients = []
    unique_patients_ids = set()
    patient_dict = {}
    
    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')
        
    lines = data.strip().split("\n")
    
    for line in lines:
        
        parts = line.strip().split("|")
        print(parts)
        if line.startswith("2P"):
            patient_id = parts[1].strip()
            
            if patient_id not in unique_patients_ids:
                patient_dict[patient_id] = {}
                unique_patients_ids.add(patient_id)

        if line.startswith("4R"):
            try:
                test_name = parts[2].strip().split("^")[3]
                test_value = parts[3].strip()
                if test_name and test_value:
                    patient_dict[patient_id][test_name] = test_value
            except IndexError as e:
                print(" IndexError in R line:", e)
                continue
    
    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))
    

    return patients

def extract_data_from_c_311(data):
    MACHINE_NAME = 'C_311'
    patients = []
    patient_dict = {}
    patient_id = None

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    lines = data.strip().split('\n')

    for line in lines:
        line = line.strip()
        parts = line.split('|')

        if line.startswith('P|'):
            patient_id = parts[1].strip() 
            if patient_id not in patient_dict:
                patient_dict[patient_id] = {}

        elif line.startswith('R|'):
            try:
                test_name = parts[2].strip().replace("^", "").replace("/", "")
                test_value = parts[3].strip()

                if patient_id and test_name and test_value:
                    patient_dict[patient_id][test_name] = test_value

            except IndexError as e:
                print("IndexError in R line:", e)
                continue

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_celltak(data):
    MACHINE_NAME = 'CELLTAK'
    patients = []
    patient_dict = {}
    patient_id = None

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    lines = data.strip().split('\n')

    for line in lines:
        line = line.strip()
        parts = line.split('|')

        if line.startswith('P|'):
            print("\n\n->line:", line)
            print("parts:" , parts)
            patient_id = parts[4].strip() 
            print(patient_id)
            if patient_id not in patient_dict:
                patient_dict[patient_id] = {}

        elif line.startswith('R|'):
            try:
                test_name = parts[2].replace('^', '').replace('/', '').strip()
                test_value = parts[3].strip()
                
                if "HIST" in test_name.upper():
                    continue

                if test_name and test_value:
                    patient_dict[patient_id][test_name] = test_value
            except IndexError as e:
                print("IndexError in R line:", e)
                continue

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients
    
def extract_data_from_electra_pro_m(data):
    MACHINE_NAME = 'ELECTRA_PRO_M'
    patients = []
    patient_dict = {}
    patient_id = None

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')
        
    blocks = data.strip().split("H|")  

    for block in blocks:
        lines = block.strip().split('\n')

        for line in lines:
            line = line.strip()
            parts = line.split('|')

            if line.startswith('O|'):
                patient_id = parts[2].strip() or 'unknown'
                if patient_id not in patient_dict:
                    patient_dict[patient_id] = {}

            elif line.startswith('R|'):
                try:
                    test_name = parts[2].strip().split('^')[-1]
                    test_value = parts[3].strip()
                    if test_name and test_value:
                        patient_dict[patient_id][test_name] = test_value
                except IndexError:
                    continue

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_fincare(data):
    MACHINE_NAME = 'FINCARE'
    patients = []
    patients_dict = {}
    current_patient_id = None
    current_tests = {}

    lines = data.strip().split("\n")

    for line in lines:
        parts = line.strip().split("|")

        if line.startswith("PID"):
            # Save previous patient's tests before starting new block
            if current_patient_id:
                if current_patient_id not in patients_dict:
                    patients_dict[current_patient_id] = {}
                patients_dict[current_patient_id].update(current_tests)

            current_patient_id = parts[5].strip()
            current_tests = {}

        elif line.startswith("OBX") and current_patient_id:
            test_names = parts[4].strip().split()
            test_values = parts[5].strip().split()

            filtered_test_values = []
            for i in range(0, len(test_values), 2):
                filtered_test_values.append(test_values[i])

            for name, val in zip(test_names, filtered_test_values):
                current_tests[name] = val

    # Store last patient's data
    if current_patient_id:
        if current_patient_id not in patients_dict:
            patients_dict[current_patient_id] = {}
        patients_dict[current_patient_id].update(current_tests)

    for pid, tests in patients_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_hema_580(data):
    MACHINE_NAME = 'HEMA580'
    patients = []
    patient_id = None
    patient_dict = {}

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    lines = data.strip().split('\n')

    for line in lines:
        line = line.strip()
        parts = line.split('|')
        print(parts)

        if line.startswith('3O'):
            patient_id = parts[2].strip() or 'UNKNOWN'

        if patient_id and patient_id not in patient_dict:
            patient_dict[patient_id] = {}

        if 'R|' in line and line.index('R|') <= 2:
            test_field = parts[2]
            test_value = parts[3].strip()

            if test_value and test_value != '---':
                if '^^^' in test_field:
                    test_name_parts = test_field.split('^')
                    if len(test_name_parts) >= 4:
                        test_name = test_name_parts[3]
                    else:
                        test_name = test_field
                else:
                    test_name = test_field

                patient_dict[patient_id][test_name] = test_value

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_mindray_bc_5130(data):
    MACHINE_NAME = 'BC_5130'
    patients = []
    patient_dict = {}
    patient_id = None

    lines = data.strip().split('\n')

    for line in lines:
        line = line.strip()
        parts = line.split('|')

        if line.startswith('PID|'):
            patient_id = parts[3].strip().split("^")[0] if len(parts) > 3 else 'UNKNOWN'
            if patient_id not in patient_dict:
                patient_dict[patient_id] = {}

        if line.startswith('OBX|') and len(parts) >= 6 and parts[2] == 'NM':
            test_field = parts[3]  
            test_value = parts[5].strip()

            if test_field and '^' in test_field:
                test_name = test_field.split('^')[1].strip()
            else:
                test_name = test_field.strip()

            if test_name and test_value:
                patient_dict[patient_id][test_name] = test_value

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

def extract_data_from_snibe(data):

    MACHINE_NAME = 'SNIBE'
    patients = []
    unique_patient_ids = set()

    control_chars = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17', '\x1c', '\x0b']
    for sym in control_chars:
        data = data.replace(sym, '')

    data_blocks = data.strip().split("MSH")

    for block in data_blocks:
        block = block.strip()
        if not block:
            continue

        lines = block.split("\n")
        # print("\n->lines", lines)
        patient_id = None
        patients_dict = {}

        for i, line in enumerate(lines):
            segment = line.strip().split("|")
            # print("==================> segment", segment)
            if line.startswith("SPM"):
                patient_id = segment[2].strip().split("^")[0]
                # print("==================> patient_id", patient_id)
                if patient_id not in patients_dict:
                    patients_dict[patient_id] = {}

            if line.startswith("OBX"):
                if len(segment) > 4:
                    test_name = segment[3].strip()
                    test_value = segment[5].strip().split("^")[0]
                    # print("==================> test name", test_name)
                    # print("==================> test value", test_value)

                    patients_dict[patient_id][test_name] = test_value

        if patient_id not in unique_patient_ids :
            tup = (MACHINE_NAME, patient_id, str(patients_dict[patient_id]))
            patients.append(tup)
            unique_patient_ids.add(patient_id)

    return patients

def extract_data_from_xn_330(data):
    MACHINE_NAME = 'XN_330'
    patient_id = 'UNKNOWN'
    patient_dict = {}

    lines = data.strip().split('\n')

    for line in lines:
        line = line.strip()
        parts = line.split('|')

        if line.startswith('P|') and len(parts) > 1:
            patient_id = parts[1].strip() or 'UNKNOWN'

        if line.startswith('R|') and len(parts) >= 4:
            test_field = parts[2].strip()
            test_value = parts[3].strip()

            if test_value == '---.-' or not test_value:
                continue

            if '^' in test_field:
                test_name_parts = test_field.split('^')
                test_name = test_name_parts[4].strip() if len(test_name_parts) >= 5 else test_field.strip()
            else:
                test_name = test_field.strip()

            if patient_id not in patient_dict:
                patient_dict[patient_id] = {}
            patient_dict[patient_id][test_name] = test_value

    results = []
    for pid, tests in patient_dict.items():
        results.append((MACHINE_NAME, pid, str(tests)))

    return results
    
    
    
def process_all_files(folder_path):
    final_result = []

    extractors = {
        'access_2': extract_data_from_access_2,
        'atul': extract_data_from_atul,
        'au_480': extract_data_from_au_480,
        'c_111': extract_data_from_c_111,
        'c_311': extract_data_from_c_311,
        'celltak': extract_data_from_celltak,
        'electra_pro_m': extract_data_from_electra_pro_m,
        'fincare': extract_data_from_fincare,
        'hema_580': extract_data_from_hema_580,
        'mindray_bc_5130': extract_data_from_mindray_bc_5130,
        'snibe': extract_data_from_snibe,
        'xn_330': extract_data_from_xn_330,
    }

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        file_data = read_file(file_path)

        if not file_data:
            continue

        print(f" Processing file: {file_name}")

        extractor = None  

        for prefix, ext in extractors.items():
            if file_name.lower().startswith(prefix.lower()):
                extractor = ext
                break 

        if extractor:
            print("Extractor found", extractor)
            results = extractor(file_data)
            print("Extractor called", results)

            if isinstance(results, list):
                for result in results:
                    if send_data_to_mysql_simple([result]):
                        try:
                            backup_path = os.path.join(backup_folder_path, file_name)
                            shutil.copy(file_path, backup_path)
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            else:
                                print(f" File already deleted: {file_path}")
                        except Exception as e:
                            print(f"Error during backup/delete: {e}")
                        final_result.append(result)
            else:
                try:
                    backup_path = os.path.join(backup_folder_path, file_name)
                    shutil.copy(file_path, backup_path)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    else:
                        print(f" File already deleted: {file_path}")
                except Exception as e:
                    print(f"Error during backup/delete: {e}")
        else:
            try:
                print(" No extractor matched for:", file_name)
                backup_path = os.path.join(backup_folder_path, file_name)
                shutil.copy(file_path, backup_path)
                if os.path.exists(file_path):
                    os.remove(file_path)
                else:
                    print(f" File already deleted: {file_path}")
            except Exception as e:
                print(f"Error during backup/delete: {e}")

    return final_result


while True:
    all_results = process_all_files(folder_path)
    for result in all_results:
        print(" Data Saved (Insert/Merge):", result)
    time.sleep(3)                  
                        
                        
            

        
        
    
        
        
 
 
   

