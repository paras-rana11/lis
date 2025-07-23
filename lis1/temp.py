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
            print("\nstarting data:", data)

            try:
                converted_new_test_to_json = test.replace("'", '"')
                print("converted_new_test_to_json:", converted_new_test_to_json, type(converted_new_test_to_json))
                new_test = json.loads(converted_new_test_to_json)
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
                    converted_old_test_to_json = existing_json_str.replace("'",'"')
                    print("converted_old_test_to_json:", converted_old_test_to_json, type(converted_old_test_to_json))                    
                    old_test = json.loads(existing_json_str)
                    print("\n\nold_test:", old_test, type(old_test))
                    print("new_test:", new_test, type(new_test))
                    
                    # if converted_old_test_to_json == converted_new_test_to_json:
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
                            # conflict = True
                            # break
                            
                    # print("-> conflict:", conflict)
                    # if not conflict:
                        merged = old_test.copy()
                        merged.update(new_test)
                        is_partial_merge = True  
                        print("\nmerged: ", merged)
                        merged_test_json = json.dumps(merged, separators=(',', ':'))
                        print("merged_test_json: ", merged_test_json)
                        print(f"-- Merging data for patient '{patientid}': {merged}")
                        break   

                        

                except Exception as e:
                    print(f"\n!! Error parsing existing DB JSON for patient '{patientid}': {e}")
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
                print(f"\n-- Database updated with merged data for patient '{patientid}'")

            else:
                normalized_json = json.dumps(new_test, separators=(',', ':'))
                cur.execute("""
                    INSERT INTO MACHINE_DATA (machinename, patientid, test) 
                    VALUES (%s, %s, %s)
                """, (machinename, patientid, normalized_json))
                print(f"\n-- New test record inserted for patient '{patientid}'")

            
            
        con.commit()
        cur.close()

    except Exception as e:
        print("---- Error during DB operation:", e)
        return False

    finally:
        con.close()

    return True


# =======================================================================================================================================================================================#

data1 = """

MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||jamila||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  eGA(ADA)1  nova |9.3 %  222.2 md/dl  27.2 mg|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F




MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||jamila||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  eGA(ADA)   HBA1C(IFCC)|9.3 %  220.2 md/dl  78.1 mmol/mol↑|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F


MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||jamila1||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  eGA(ADA)   HBA1C(IFCC)|7.3 %  7.2 md/dl  98.1 mmol/mol↑|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F


MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||jamila||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  figma1   HBA1C(IFCC)|9.3 %  222.2 md/dl  78.1 mmol/mol↑|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F


MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||jamila||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  figma1   HBA1C(IFCC)|9.4 %  272.2 md/dl  73.1 mmol/mol↑|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F


MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||3rd||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  eGA(ADA)1  nova |9.3 %  222.2 md/dl  27.2 mg|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F


MSH|^~\&|FineCarePlus|飞测ⅡPlus^00:21:9B:1A:E0:4A^FS-113|POCT|POCT_SERVER^^PC|20240831111831||ORU^R01^ORU_R01|202408210001|P|2.4||||0|CHN|UTF8
PID|202408210001||||4thhhhhhhh||19570831|F
OBR|202408210001|F2071A20E|1||||20240821113113|20240821113113|||||||Whole blood|sridhar ramadasu 
OBX|202408210001|NM||HbA1c  eGA(ADA)1  nova |9.3 %  2.2 md/dl  27.2 mg|%|0.0-6.5||||||9.3|20240821113113
DSC|1|F

"""

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


output = extract_data_from_fincare(data1)
print("\n=> after calling func- extract_data_from_fincare: ")
for op in output:
    print(op)
    
res = send_data_to_mysql_simple(output)
print("\n=> after calling send data to mysql func: ")
print("res: ", res)



