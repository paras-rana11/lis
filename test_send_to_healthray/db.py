import json
import os
import time
import pymysql
import requests
import re
import jwt
import datetime
import ast
import logging
import threading

# Configuration
# my_host = '127.0.0.1'
# my_user = 'root'
# my_pass = 'Root@1234'
# my_db = 'lis'
folder_path = 'C:\\All\\LIS\\test_send_to_healthray\\report_file'
LAB_BRANCH_ID = 97
JWT_SECRET_KEY = 'S1@sCHJ%fK*y8$=f^Bl3gF8isQLS&$d1'



def read_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        return None

def get_connection():
    try:
        con = pymysql.connect(
            host=my_host,
            user=my_user,
            password=my_pass,
            database=my_db
        )
        return con
    except pymysql.MySQLError as e:
        print(f"Connection error: {e}")
        return None

def run_query(con, prepared_sql, data_tpl):
    try:
        with con.cursor() as cur:
            cur.execute(prepared_sql, data_tpl)
            con.commit()
            return cur
    except pymysql.MySQLError as e:
        print(f"Query error: {e}")
        return None

def close_cursor(cur):
    try:
        if cur:
            cur.close()
    except pymysql.MySQLError as e:
        print(f"Cursor close error: {e}")

def close_connection(con):
    try:
        if con:
            con.close()
    except pymysql.MySQLError as e:
        print(f"Connection close error: {e}")

def send_to_mysql(data):
    try:
        con = get_connection()
        if not con:
            return False

        cursor = con.cursor()
        query = "SELECT test FROM machineData WHERE patientId = %s AND machineName = %s"
        cursor.execute(query, (data[1], data[0]))
        result = cursor.fetchone()
        print(result)
        try:
            new_data_dict = data[2] if data[2] else {}

            if result:
                # Parse the existing JSON from DB
                existing_data_dict = json.loads(result[0]) if result[0] else {}

                # Merge old and new data
                existing_data_dict.update(new_data_dict)

                # Save merged data
                merged_data_json = json.dumps(existing_data_dict)

                update_sql = 'UPDATE machineData SET test = %s WHERE patientId = %s AND machineName = %s'
                cur = run_query(con, update_sql, (merged_data_json, data[1], data[0]))
            else:
                # New entry
                insert_sql = 'INSERT INTO machineData (machineName, patientId, test) VALUES (%s, %s, %s)'
                merged_data_json = json.dumps(new_data_dict)
                cur = run_query(con, insert_sql, (data[0], data[1], merged_data_json))

            if cur:
                close_cursor(cur)

        except Exception as e:
            # logger.error(f"Error while saving data in db :- {e}")
            return False

        finally:
            close_connection(con)

        return True

    except Exception as e:
        # logger.error(f"Exception during insert data in db :- {e}")
        return False




def extract_report_data_mindray_bc_5150(data):
    lines = data.split('\n')
    patientId = None
    machine_name = "BC_5150"
    report_data = {}
    patients = []
    formatted_data = []
    
    obx_data = ""
    
    for line in lines:
        if line.startswith("OBX"):
            if obx_data:
                formatted_data.append(obx_data.strip()) 
            obx_data = line  
        elif obx_data and line.strip(): 
            obx_data += "" + line.strip()
        else:
            if obx_data:
                formatted_data.append(obx_data.strip()) 
                obx_data = "" 
            formatted_data.append(line)  
    if obx_data:
        formatted_data.append(obx_data.strip())

    for line in formatted_data:
        if line.startswith('OBR'):
            # print(line)
            if patientId and report_data:
                patients.append((machine_name, patientId, report_data))
                report_data = {}
                patientId = None
            patientId = line.split('|')[3].replace('^MR', '').replace('^', '')
        elif line.startswith('OBX'):
            if line.split('|')[2] == 'IS':
                continue
            report_name = line.split('|')[3]
            report_value = line.split('|')[5]
            report_data[report_name] = report_value

    if patientId and report_data:
        report_data = str(report_data)
        patients.append((machine_name, patientId, report_data))

    return patients

def extract_report_data_mispa_fab_120(data):
    lines = data.split('\n')
    patientId = None
    machine_name = "FAB_120"
    report_data = {}
    patients = []
    formatted_data = []
    
    obx_data = ""
    
    for line in lines:
        if line.startswith("OBX"):
            if obx_data:
                formatted_data.append(obx_data.strip()) 
            obx_data = line  
        elif obx_data and line.strip(): 
            obx_data += "" + line.strip()
        else:
            if obx_data:
                formatted_data.append(obx_data.strip()) 
                obx_data = "" 
            formatted_data.append(line)  
    if obx_data:
        formatted_data.append(obx_data.strip())

    for line in formatted_data:
        if line.startswith('OBR'):
            # print(line)
            if patientId and report_data:
                patients.append((machine_name, patientId, report_data))
                report_data = {}
                patientId = None
            patientId = line.split('|')[3].replace('^MR', '').replace('^', '')
        elif line.startswith('OBX'):
            if line.split('|')[2] == 'IS':
                continue
            report_name = line.split('|')[3].strip()
            report_value = line.split('|')[5]
            report_data[report_name] = report_value

    if patientId and report_data:
        report_data = str(report_data)
        patients.append((machine_name, patientId, report_data))

    return patients

def extract_report_data_access_2(access_data):
    lines = access_data.strip().split('\x02')
    machine_name, patient_name, test_name, test_result = 'ACCESS', 'N/A', 'N/A', 'N/A'
    final_list = []
    patientId = None
    for line in lines:
        if line.startswith('1H'):
            machine_name = line.split('|')[4]
        elif line.startswith('3O'):
            rerun_case_no = line.split('|')[2]
            raw_patientId = line.split('|')[2]
            if '-' in raw_patientId:
                patientId = raw_patientId.split('-')[0] 
            else:
                patientId = raw_patientId
        elif line.startswith('4R'):
            test_data = line.split('|')
            test_name = test_data[2].replace('^', '').rstrip('1')
            test_result = test_data[3].replace('>', '').replace('<', '')
    final_list.append((machine_name, patientId, str({test_name: test_result}), rerun_case_no))
    return final_list

def extract_report_data_au_480(au_data):
    machine_name, patient_name = "AU_480", "N/A"
    final_list = []
    sub_data = au_data.strip().split()
    if len(sub_data) > 3:
        rerun_case_no = sub_data[3]
        raw_patientId = sub_data[3]
        if '-' in raw_patientId:
            patientId = raw_patientId.split('-')[0] 
        else:
            patientId = raw_patientId
    pattern = r'(\d{3})\s+(\d+\.\d+)'
    matches = re.findall(pattern, au_data)
    result = {key: value for key, value in matches}
    
    final_list.append((machine_name, patientId, str(result), rerun_case_no))
    return final_list

# Method for Mindray BS 240 Machie 
def extract_report_data_mindray_bc_5130(data):
    lines = data.split('\n')
    patientId = None
    machine_name = "Mindray_BC_5130"
    report_data = {}
    patients = []
    for line in lines:

        if line.startswith('PID'):
            if patientId and report_data:
                patients.append((machine_name, patientId, report_data, rerun_case_no))
                report_data = {}
                patientId = None
            rerun_case_no= line.split('|')[3].split('^')[0]
            raw_patientId = line.split('|')[3].replace('^MR', '').replace('^', '')
            if '-' in raw_patientId:
                patientId = raw_patientId.split('-')[0] 
            else:
                patientId = raw_patientId
        elif line.startswith('OBX'):
            if line.split('|')[2] == 'IS':
                continue
            report_name = line.split('|')[3].split('^')[1]
            report_value = line.split('|')[5]
            report_data[report_name] = report_value

    if patientId and report_data:
        patients.append((machine_name, patientId, str(report_data), rerun_case_no))

    return patients

def extract_report_data_accre_8(text):
    reports = text.split("MSH")  # Split the data by the 'MSH' delimiter to separate each report
    machine_name = "accre_8"
    extracted_data = []

    for report in reports:
        if not report.strip():  # Skip any empty or whitespace-only parts
            continue
        
        lines = report.split("\n")
        pid = ''
        report_data = {}
        
        for line in lines:
            if line.startswith('PID'):
                pid = line.split('|')[5]
            elif line.startswith('OBX'):
                test_name = line.split('|')[4]
                test_result = line.split('|')[5]
                report_data[test_name] = test_result
        
        if pid and report_data:  # Ensure we have valid data before adding it to the list
            extracted_data.append((machine_name, pid, str(report_data)))

    return extracted_data

def remove_control_characters(text):
    try:
        block_number = 1
        while True:
            pattern = r"\x02" + str(block_number)
            if not re.search(pattern, text):
                break
            text = re.sub(pattern, "", text, count=1)
            block_number += 1

        # Step 2: Remove ENQ, ETX+1char, EOT, ETB+2char
        text = re.sub(r"\x05", "", text)   
        text = re.sub(r"\x03.", "", text)  
        text = re.sub(r"\x04", "", text)       
        text = re.sub(r"\x17..", "", text)  

        # Strip spaces from each line
        text = "\n".join(line.strip() for line in text.splitlines())
        return text
    except Exception as e:
        # error_message = 'Error in remove control characters'
        # send_mail_or_logging(error_message, e)
        return None

def merge_broken_lines(text):
    try:
        lines = text.splitlines()
        merged_lines = []
        fixed_lines = []

        for line in lines:
            line = line.strip()
            if merged_lines and not re.match(r'^[A-Z]\|\d+\|', line) and not re.match(r'^[HLPRCO]\|', line):
                merged_lines[-1] += line  # continuation of previous line
            else:
                merged_lines.append(line)

        for line in merged_lines:
            if re.match(r'^C\|1\|I\|.*R\|\d+\|', line):
                # Find the part where "R|n|" starts
                match = re.search(r'(R\|\d+\|)', line)
                if match:
                    split_index = match.start()
                    corrupted_c = line[:split_index]
                    fixed_r = line[split_index:]
                    fixed_lines.append(corrupted_c)
                    fixed_lines.append(fixed_r)
                else:
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)    

        return fixed_lines

    except Exception as e:
        # error_message = 'Error in merge broken line function'
        # send_mail_or_logging(error_message, e)
        return []

def is_numeric(val):
    try:
        float(val)
        return True
    except ValueError:
        return False

def extract_report_data_c_311_1(data):
    try:
        machine_name = 'C_311_1'
        pid_field = None  # Initialize with a default value
        final_list = []
        result_dict = {}

        # Step 1: Clean control characters
        cleaned = remove_control_characters(data)

        # Step 2: Merge broken lines
        merged_lines = merge_broken_lines(cleaned)
        for line in merged_lines:
            if line.__contains__('O'):
                if pid_field and result_dict:
                    final_list.append((machine_name, pid_field, result_dict))
                    result_dict = {}
                    pid_field = None

                pid_field = line.split("|")[2].strip()
                # if len(pid_field) > 1:
                #     pid_field =pid_field[1]
                # else:
                #     pid_field =pid_field[0]
            
                # if '-' in pid_field:
                #     pid_field = pid_field.split('-')[0].strip()
                # if 'H' in pid_field or 'h' in pid_field or 'P' in pid_field or 'p' in pid_field:
                #     pid_field = re.sub(r'[HhPp]', '', pid_field).strip()

            elif line.startswith('R') or line.startswith('V'):
                if '{' in line or '}' in line or 'o' in line or 'O' in line:
                    line = line.replace('{','|').replace('}','|').replace('o','/').replace('O','/')

                parts = line.split('|')
                if len(parts) >= 4:
                    test_id = parts[3].split('^')[-1].replace('/','').strip()
                    if '\x02' in test_id:
                        test_id = test_id.split('\x02')[1]

                        if " " in test_id:
                            test_id = test_id.split()[1]

                    test_value = parts[4]
                    if '\x02' in test_value:
                        test_value = parts[4].split('\x02')[1]

                    if is_numeric(test_value):
                        result_dict[test_id] = test_value

        if pid_field and result_dict:
            final_list.append((machine_name, pid_field, result_dict))

        return final_list
    except Exception as e:
        pass

def extract_report_data_c_111(data):
    # Use fixed 'c111' value
    c111 = 'c111'
    lines = data.split("\n")
    pid = ''
    final_list = []
    
    for line in lines:
        if line.__contains__('3O'):
            rerun_case_no = line.split("|")[3].split("^")[0]
            raw_pid = re.sub(r'\^\d', '', line.split("|")[3]).replace("^","")
            if '-' in raw_pid:
                pid = raw_pid.split('-')[0] 
            else:
                pid = raw_pid
            # pid_field = line.split("|")[3]
            # pid = re.sub(r'\^\d+', '', pid_field).replace("^", "")
            
    # Extract results in the desired format
    result_pattern = re.compile(r"(\d)R\|\d\|\^\^\^(\d+)\|([\d.]+)\|[^|]*")
    results = result_pattern.findall(data)

    results_dict = {code: value for _, code, value in results}
    final_list.append((c111, pid, str(results_dict), rerun_case_no))
    return final_list

def extract_report_data_celltak(text):
    lines = text.split("\n")
    machine_name = "celltak_@+"
    pid = ""
    report_data = {}
    final_list = []
    for line in lines:
        if line.startswith("P"):
            rerun_case_no = line.split("|")[4]
            raw_pid = line.split("|")[4]
            if '-' in raw_pid:
                pid = raw_pid.split('-')[0] 
            else:
                pid = raw_pid
            
        elif line.startswith("R"):
            report_name = line.split("|")[2].replace("^","")
            report_value = line.split("|")[3]
            report_data[report_name] = report_value
    
    final_list.append((machine_name,pid,str(report_data), rerun_case_no))
    return final_list

def extract_report_data_nx_600(text):
    data = text.split(",")
    pid = data[5].strip()

    report_data = {}
    final_list = []
    i = 12  
    while i < len(data):
        if i + 2 < len(data):
            test_name = data[i].strip()
            test_result = data[i+2].split()[0]
            report_data[test_name] = test_result
            i += 7
        else:
            break
    final_list.append(("fuji_film",pid,str(report_data)))
    return final_list

def extract_report_data_maglumi(text):
    
    machine_name = "snibe"
    reports = text.strip().split("\n\n")  
    final_list = []
    for report_block in reports:
        lines = report_block.split("\n")
        pid = ""
        report = {}
        
        for line in lines:
            if line.startswith("SPM"):
                data_pid = line.split("|")
                pid = data_pid[2].replace("^", "")
            elif line.startswith("OBX"):
                data_report = line.split("|")
                report_name = data_report[3]
                report_data = data_report[5].split("^")[0]
                
                if report_name not in report:
                    report[report_name] = []
                report[report_name].append(float(report_data))
        
        final_list.append((machine_name,pid,str(report)))
    return final_list 

def extract_report_data_from_cobas_machine(data, file_prefix):
    final_list = []
    try:
        # print(data)
        machine_name = file_prefix.upper()
        pid_field = None  # Initialize with a default value
        result_dict = {}

        # Step 1: Clean control characters
        cleaned_data = remove_control_characters(data)

        # Step 2: Merge broken lines
        merged_lines = merge_broken_lines(cleaned_data)

        for line in merged_lines:
            if line.__contains__('O'):
                if pid_field and result_dict:
                    final_list.append((machine_name, pid_field, str(result_dict)))
                    result_dict = {}
                    pid_field = None

                pid_field = line.split("|")[2].strip()
                if '-' in pid_field:
                    pid_field = pid_field.split('-')[0].strip()
                if 'H' in pid_field or 'h' in pid_field or 'P' in pid_field or 'p' in pid_field:
                    pid_field = re.sub(r'[HhPp]', '', pid_field).strip()

            elif line.startswith('R') or line.startswith('V'):
                # print(line)
                if '{' in line or '}' in line or 'o' in line or 'O' in line:
                    line = line.replace('{','|').replace('}','|').replace('o','/').replace('O','/')

                parts = line.split('|')
                if len(parts) >= 4:
                    test_id = parts[2].split('^')[-1].replace('/','').strip()
                    
                    if '\x02' in test_id:
                        test_id = test_id.split('\x02')[1]

                        if " " in test_id:
                            test_id = test_id.split()[1]

                    test_value = parts[3]
                    if '\x02' in test_value:
                        test_value = parts[3].split('\x02')[1]

                    if is_numeric(test_value):
                        result_dict[test_id] = test_value
        # print(result_dict)
        if pid_field and result_dict:
            final_list.append((machine_name, pid_field, str(result_dict)))
        # print(final_list)
        return final_list
    except Exception as e:
        # error_message = 'Error in extract data from c311 2'
        # send_mail_or_logging(error_message, e)
        return final_list




def extract_list_data_from_txt_file(folder_path):
    file_prefix = None
    extractor = None
    try:
        results = []
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            text_data = read_file(file_path)
            if not text_data:
                continue
            extractors = {
                'access_2_': extract_report_data_access_2,                      # rerun
                'au_480_': extract_report_data_au_480,                          # rerun
                'c_111_': extract_report_data_c_111,                            # rerun
                'celltak_': extract_report_data_celltak,                        # rerun
                'mindray_bc_5130_': extract_report_data_mindray_bc_5130,
                # 'mispa_fab_120_': extract_report_data_mispa_fab_120,
                # 'snibe_': extract_report_data_maglumi,
                # 'accre_': extract_report_data_accre_8,
                # 'fuji_film_': extract_report_data_nx_600,
                # 'c_311_1_': extract_report_data_c_311_1,
                # 'bc_5150_': extract_report_data_mindray_bc_5150,
                # 'c_311_1_':extract_report_data_from_cobas_machine
            }
            for prefix, ext in extractors.items():
                if filename.startswith(prefix):
                    file_prefix = prefix
                    extractor = ext
                    # print(file_prefix)
                    # print(extractor)
                    break
            if extractor:
                results.append(extractor(text_data))
                # print(results)
                try:
                    # os.remove(file_path)
                    file_path = None
                except Exception as e:
                    pass
            else:
                try:
                    # os.remove(file_path)
                    file_path = None
                except Exception as e:
                    pass
        
        return results
    except Exception as e:
        print(f"Exception Occur in process file:- {e}")
    finally:
        try:
            if file_path:
                # os.remove(file_path)
                file_path = None
        except Exception as e:
            pass

def send_to_healthray(results):
    try:
        url = "http://192.168.1.69:4206/api/v1/lis_case_result"

        expiry_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=50000)
        payload = {
            "data": {
                "platform": "Python",
                "branch_id": LAB_BRANCH_ID 
            },
            "exp": expiry_time
        }
        jwt_token = jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')

        # print(jwt_token)
        header = {
            'authorization': f'{jwt_token}',
            'Content-Type': 'application/json'
        }

        output_dict = {
            "patient_case_results": [
                {
                    "lab_branch_id": LAB_BRANCH_ID,
                    "lis_machine_id": sub_item[0],
                    "patient_case_no": sub_item[1],
                    # "result": sub_item[2],
                    "result": json.dumps(ast.literal_eval(sub_item[2]) if isinstance(sub_item[2], str) else sub_item[2]),
                    "rerun_case_id": sub_item[3]
                }
                for item in results for sub_item in item
            ]
        }
        # print(output_dict)
        response = requests.post(url, headers=header, json=output_dict)
        # response = {}
        # response['status_code'] =  200
        print(f"Response status code: {response.status_code}")
        print(f"Response text: {response.text}")
        if response is None:
            print("Error: No response received from server!")
            return response.json() 

        if response.status_code == 200:
            print("data Sended Succesfully")
            return response.json() 
        else:
            print(f"Error: Server returned an error. {response}")
            return response.json() 

    except Exception as e:
        print(f"Exception Occur in API Calling :- {e}")
        return None

# Method for process all file in given folder
def send_data_to_mysql(results):
    case_no_list = []
    google_data_list = []
    try:
        flattened_list = [sub_list for item in results for sub_list in item]
        if isinstance(flattened_list, list):
            for result in flattened_list:
                result = list(result)
                google_data_list.append(result)
                data_dict = result[2]
                case_no = result[1]
                data_dict = ast.literal_eval(data_dict)
                result[2] = data_dict
                result = tuple(result)
                send_to_mysql(result)       
                case_no_list.append(case_no) 
        else:
            pass
        return case_no_list, google_data_list
    except Exception as e:
        print(f"Exception Occur in Save Data in MySQL :- {e}")
        return case_no_list, google_data_list

while True:
    try:
        all_results = extract_list_data_from_txt_file(folder_path)
        if all_results:
            # case_no, google_data = send_data_to_mysql(all_results)
            # print(f"Data Save in MySQL For {case_no}")
            save_to_healthray_lab = send_to_healthray(all_results) 
            print(f"Data Save in Cloud For {save_to_healthray_lab}")
        for r in all_results:
            print(r)
        time.sleep(10)
    except Exception as e:
        logging.info(f"Exception Occur in main :- {e}")
        





# {
#     'patient_case_results': [
#         {
#             'lab_branch_id': 97, 
#             'lis_machine_id': 'Mindray_BC_5130', 
#             'patient_case_no': 'S10528', 
#             'result': "{'WBC': '4.60', 'BAS#': '0.02', 'BAS%': '0.5', 'NEU#': '2.32', 'NEU%': '50.5', 'EOS#': '0.36', 'EOS%': '7.9', 'LYM#': '1.49', 'LYM%': '32.3', 'MON#': '0.41', 'MON%': '8.8', 'RBC': '5.55', 'HGB': '16.5', 'MCV': '87.9', 'MCH': '29.8', 'MCHC': '33.9', 'RDW-CV': '14.0', 'RDW-SD': '48.1', 'HCT': '48.8', 'PLT': '241', 'MPV': '8.8', 'PDW': '16.3', 'PCT': '0.212', 'PLCC': '48', 'PLCR': '19.9'}"
#             'rerun_case_id': 'S10528-S', 
#         }, 
#         {
#             'lab_branch_id': 97, 
#             'lis_machine_id': 'AU_480', 
#             'patient_case_no': 'S10528', 
#             'result': "{'005': '4.62', '006': '236.54', '007': '23.32', '009': '32.57', '013': '0.50', '017': '0.33', '018': '14.98', '019': '187.41', '026': '1.24', '027': '7.25', '097': '130.02', '098': '4.08', '099': '93.62'}"
#         }, 
#         {
#             'lab_branch_id': 97, 
#             'lis_machine_id': 'FAB_120', 
#             'patient_case_no': 'S10528', 
#             'result': "{'ALB': '4.13', 'ALP-IFCC': '37.49', 'ALT': '12.71', 'AST': '15.27', 'CHOL': '186.75', 'D-BIL': '0.09', 'HDL-C': '77.22', 'T-BIL': '0.28', 'TGL': '47.27'}"
#         }, 
#         {
#             'lab_branch_id': 97, 
#             'lis_machine_id': 'ACCESS^508178', 
#             'patient_case_no': 'HARNISH', 
#             'result': "{'PCT': '18.487'}"
#         }, 
#         {
#             'lab_branch_id': 97, 
#             'lis_machine_id': 'BC_5150', 
#             'patient_case_no': 'S10528', 
#             'result': "{'TP': '6.78', 'UREA': '22.75', 'BUN': '63.71', 'CHOL/HDL': '2.42', 'GLOB': '2.66', 'I-BIL': '0.18', 'NON-HDL': '109.53', 'VLDL': '9.45'}"
#         }
#     ]
# }

