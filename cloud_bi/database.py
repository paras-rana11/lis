# pip install mysql-connector-python

import json
import logging
import os
import re
import time
import shutil
import threading
import mysql.connector
import smtplib
import requests
import jwt
from typing import List, Dict, Any
from mysql.connector import Error
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

# Configuration
my_host = '127.0.0.1'
my_user = 'root'
my_pass = 'root1'
my_db = 'lis'
folder_path = '/home/dhruveel/Desktop/Dhruveel/machine_integration_main_project/machine_integration_main_project/attachments'

# Report file path
REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'
BACKUP_FILE_PATH = 'C:\\ASTM\\root\\backup_file\\'
CASE_FILE_PATH = 'C:\\ASTM\\root\\case_file\\'

# log file path
LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_database.log'
ERROR_LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_database_error.log'
LOG_FILE_LIST = [LOG_FILE, ERROR_LOG_FILE]


# Email configuration
SUBJECT = ''
TO_EMAIL = ''
FROM_EMAIL = ''
PASSWORD = ''

# Cloud configuration
LAB_BRANCH_ID = None
LAB_URL = ''
JWT_SECRET_KEY = ''


# def comment():
#     stored_license_key = "34ad7747ef4f423ca6234c0a6de73eedff591ea67235ae8e84486a90ae731b56"

#     def get_mac_address():
#         mac = uuid.UUID(int=uuid.getnode()).hex[-12:]
#         return ":".join([mac[e:e + 2] for e in range(0, 11, 2)])


#     def generate_license_key(mac_address):
#         key = hashlib.sha256(mac_address.encode()).hexdigest()
#         return key


#     def check_license(license_key, mac_address):
#         generated_key = generate_license_key(mac_address)
#         return generated_key == license_key

# License check
# current_mac = get_mac_address()
# if not check_license(stored_license_key, current_mac):
#     print("Invalid license. This executable can only run on the registered system.")
#     logging.error("Invalid license. This executable can only run on the registered system.")
#     sys.exit(1)


# Method for setup logging
def setup_loggers(log_file, error_log_file):
    '''Set up separate loggers for info and error.'''
    logger = logging.getLogger('app_logger')
    try:
        logger.setLevel(logging.DEBUG)  # capture everything, filter in handlers
        logger.handlers.clear()  # avoid duplicate logs on reload

        # Info Handler
        info_handler = logging.FileHandler(log_file)
        info_handler.setLevel(logging.INFO)
        info_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        info_handler.setFormatter(info_format)

        # Error Handler
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        error_handler.setFormatter(error_format)

        logger.addHandler(info_handler)
        logger.addHandler(error_handler)

        return logger
    except Exception as e:
        print(f"Error in initialize logger : {e}")
        return logger


logger = setup_loggers(LOG_FILE, ERROR_LOG_FILE)
logger.info('Log file initialized.')
logger.error('This is an error log example.')


# Method for sending email and logging
def send_mail_or_logging(error_message, error):
    logger.error(f'{error_message} : {error}')

    body = f"""
    Dear Team,

    This is an automated alert from the Laboratory Information System.

        --> {error_message} :: Below are the details of the incident:

        - Filename: database.py
        - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        - Error Details: {error}

    Best regards,  
    Laboratory Information System  
    [Healthray LAB]
    """

    # is_send_mail = send_email(SUBJECT, body, TO_EMAIL, FROM_EMAIL, PASSWORD)
    # if not is_send_mail:
    #     logger.error(f"Exception during sending mail")


# Method for send email
def send_email(subject, body, to_email, from_email, password):
    try:
        # Create the email message
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = to_email
        msg.set_content(body)

        # Connect to Gmail SMTP server
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.ehlo()
            smtp.starttls()  # Secure the connection
            smtp.login(from_email, password)
            smtp.send_message(msg)

        logger.info('Email sent successfully!')
        return True
    except Exception as e:
        logger.error(f'Failed to send email: {e}')
        return False


def remove_old_logs_from_log_file(log_file_list):
    try:
        logger.info("Thread Start For Remove old Logs")
        while True:
            for file in log_file_list:
                with open(file, 'r') as f:
                    data = f.read()

                lines = data.split('\n')

                cutoff_date = datetime.now() - timedelta(days=10)
                final_line_list = []
                for line in lines:
                    line_date = line.split()
                    if len(line_date) > 1:
                        if line_date[0] > (str(cutoff_date)).split()[0]:
                            final_line_list.append(line)

                with open(file, 'w') as f:
                    f.write('\n'.join(final_line_list))

            time.sleep(86400)
    except Exception as e:
        error_message = 'Error in remove old log'
        send_mail_or_logging(error_message, e)


def read_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        return None


def get_connection():
    try:
        con = mysql.connector.connect(
            host=my_host,
            user=my_user,
            password=my_pass,
            database=my_db
        )
        if con.is_connected():
            return con
    except Error as e:
        return None


def run_query(con, prepared_sql, data_tpl):
    try:
        print("Executing SQL:", prepared_sql)
        print("With data:", data_tpl)
        cur = con.cursor()
        cur.execute(prepared_sql, data_tpl)
        con.commit()
        print("Rows affected:", cur.rowcount)
        logger.info(f"Inserted rows: {cur.rowcount}")
        return cur
    except Exception as e:
        logger.error(f"run_query failed: {e}")
        print(f"run_query failed: {e}")
        return None




def close_cursor(cur):
    try:
        cur.close()
    except Exception as e:
        pass


def close_connection(con):
    try:
        con.close()
    except Exception as e:
        pass


# Method for save data in mysql database
def send_to_mysql(data):
    con = get_connection()
    if not con:
        print("sql not connected")
        return False
    print("sql con connected")
    prepared_sql = 'INSERT INTO machine_Data (machineName, patientId, test) VALUES (%s, %s, %s)'
    try:
        # if data[2] coming as dict then convert it to string
        if isinstance(data[2], dict):
            test_data = json.dumps(data[2]) if data[2] else json.dumps({})
        else:
            test_data = data[2]

        final_data = (data[0], data[1], test_data)


        print("Final data going to SQL:", (data[0], data[1], data[2]))

        cur = run_query(con, prepared_sql, final_data)
        logger.info(f"data added to local sql: {data}")
        for handler in logger.handlers:
            handler.flush()
        if cur:
            close_cursor(cur)
    except Exception as e:
        return False
    finally:
        close_connection(con)
    return True




# Method for Cobas C 311 Machine
def extract_report_data_c_311(data):
    c311 = 'c311'
    lines = data.split("\n")
    pid_field = None  # Initialize with a default value
    final_list = []
    result_dict = {}
    formatted_lines = []
    buffer = ""

    for line in lines:
        line = line.strip()
        if line.startswith(("R|", "O|", "P|", "L|", "C|")):
            if buffer:
                formatted_lines.append(buffer)
                buffer = ""
            buffer = line
        else:
            buffer += line
    if buffer:
        formatted_lines.append(buffer)

    for line in formatted_lines:
        if line.__contains__('O'):
            pid_field = line.split("|")[2].split('-')[0].strip()
        elif line.startswith('R'):
            parts = line.split('|')
            if len(parts) >= 4:
                test_id = parts[2].replace('^', ' ').replace('/', ' ').strip()
                if '\x02' in test_id:
                    test_id = test_id.split('\x02')[1]
                test_value = parts[3]
                result_dict[test_id] = test_value

    if pid_field:
        final_list.append((c311, pid_field, result_dict))
    return final_list


# Method for remove -1 and 1 for Cobas E 411
def convert_values(input_dict):
    output_dict = {}
    for key, value in input_dict.items():
        # Handle values with '-1^' or '1^'
        if '-1^' in value:
            value = value.replace('-1^', '')
        elif '1^' in value:
            value = value.replace('1^', '')
        output_dict[key] = value
    return output_dict

# Method for Cobas E 411 Machine
def extract_report_data_e_411(hl7_message):
    machine_id = 'e_411'
    final_list = []
    patient_id_match = re.search(r'3O\|1\|([^|]+)', hl7_message)
    patient_id = patient_id_match.group(1) if patient_id_match else None
    results = re.findall(r'R\|\d+\|\^\^\^(\d+)\^\^[^\|]+\|([^\|]+)', hl7_message)
    formatted_results = {result[0]: result[1] for result in results}
    converted_results = convert_values(formatted_results)
    final_list.append((machine_id, patient_id, converted_results))
    return final_list
    # return (machine_id, patient_id, converted_results)


# Method for Beckman Coulter Access 2 Machine
def extract_report_data_access_2(access_data, mid):
    lines = access_data.strip().split('\x02')
    machine_name, patient_name, test_name, test_result = mid, 'N/A', 'N/A', 'N/A'
    final_list = []
    for line in lines:
        # if line.startswith('1H'):
        #     machine_name = line.split('|')[4]
        if line.startswith('3O'):
            patient_name = line.split('|')[2]
        elif line.startswith('4R'):
            test_data = line.split('|')
            test_name = test_data[2].replace('^', '').rstrip('1')
            test_result = test_data[3].replace('>', '').replace('<', '')
    final_list.append((machine_name, patient_name, {test_name: test_result}))
    return final_list
    # return (machine_name, patient_name, {test_name: test_result})


# Method for Agappe Mispa Machine
def extract_report_data_mispa_nano(text):
    lines = text.split('\n')
    formatted_data = []

    obx_data = ""
    for line in lines:
        # Check if line starts with "R", indicating a new OBX segment
        if line.startswith("R"):
            if obx_data:
                formatted_data.append(obx_data.strip())
            obx_data = line  # Initialize with the current line
        elif obx_data and line.strip():
            # Join the current line to the OBX data, ensuring proper spacing
            obx_data += "|" + line.strip()
        else:
            if obx_data:
                # Append the final OBX data segment
                formatted_data.append(obx_data.strip())
                obx_data = ""
            # Append non-OBX lines directly to the formatted data
            formatted_data.append(line)

    # Append any remaining OBX data
    if obx_data:
        formatted_data.append(obx_data.strip())

    if formatted_data:
        machine_name = 'mispa'
        patients = []  # Store multiple patient records
        patient_name = None
        report_data = {}

        for line in formatted_data:
            if line.startswith('O'):
                if patient_name and report_data:
                    patients.append((machine_name, patient_name, report_data))
                    report_data = {}
                    patient_name = None
                patient_name = re.sub(r"^(\d+)(\/[A-Za-z])$", r"\1", line.split('|')[3])
            elif line.startswith('R'):
                report_name = line.split('|')[2].split('^')[1]
                report_value = line.split('|')[3].replace('^', '').strip()
                report_data[report_name] = report_value

        # Append the last patient's data after the loop
        if patient_name and report_data:
            patients.append((machine_name, patient_name, report_data))

        return patients


# Method for Mindray BS 240 Machie
def extract_report_data_mindray_bs_240(data):
    lines = data.split('\n')
    machine_name = 'BS_240'
    patientId = None
    report_data = {}
    patients = []
    for line in lines:
        if line.startswith('O'):
            if patientId and report_data:
                patients.append((machine_name, patientId, report_data))
                report_data = {}
                patientId = None
            if len(line.split('|')) >= 3:
                patientId = line.split('|')[3].strip()
        elif line.startswith('R'):
            if len(line.split('|')) >= 2:
                report_name = line.split('|')[2].replace('^F', '').replace('^', '')
                report_value = line.split('|')[3].replace('^', '')
                report_data[report_name] = report_value

    if patientId and report_data:
        patients.append((machine_name, patientId, report_data))

    return patients


def backup_and_remove_file(file_path: str, backup_path: str):
    try:
        shutil.copy2(file_path, backup_path)
        os.remove(file_path)
        logger.info(f"Backed up and removed file: {file_path}")
    except Exception as e:
        if 'The process cannot access the file because it is being used by another process' not in str(e):
            send_mail_or_logging('Error in removing or backing up file', e)


# Method for process all file in given folder
def process_all_files(folder_path, backup_path):
    try:
        final_results = []
        file_path = None
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            text_data = read_file(file_path)
            if not text_data:
                continue
            extractors = {
                'c_311_': extract_report_data_c_311,
                'e_411_': extract_report_data_e_411,
                'access_2_1': extract_report_data_access_2,
                'access_2_2': extract_report_data_access_2,
                'mispa_nano_': extract_report_data_mispa_nano,
                'bs_240_': extract_report_data_mindray_bs_240,
            }

            prefix, extractor = next(((prefix, ext) for prefix, ext in extractors.items() if filename.startswith(prefix)), None)
            try:
                print('prefix: ', prefix)
                print("extractor:", extractor)
                if prefix in ['access_2_1', 'access_2_2']:
                        results = extractor(text_data, prefix)
                else:
                        results = extractor(text_data)
            except Exception as e:
                    send_mail_or_logging(f"Error in extractor for {filename}", e)
                    backup_and_remove_file(file_path, backup_path)
                    continue

            if isinstance(results, list):
                    for result in results:
                        if send_to_mysql(result):
                            backup_and_remove_file(file_path, backup_path)
                            final_results.append(result)
                           
            else:
                logger.warning(f"No extractor matched for file: {filename}")
                backup_and_remove_file(file_path, backup_path)
                
        return final_results
    except Exception as e:
        error_message = 'Error in extract data from txt file'
        send_mail_or_logging(error_message, e)
        return final_results


# def old_code():
#     while True:
#         all_results = process_all_files(folder_path)
#         print("yep!")
#         for result in all_results:
#             print(f"Data Inserted :- {result}")
#         time.sleep(2)


# Load JSON with machine_id-style format
def load_json(file_path):
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as json_file:
                try:
                    existing_data = json.load(json_file)
                    if not isinstance(existing_data, dict):
                        existing_data = {}
                except json.JSONDecodeError:
                    existing_data = {}
        else:
            existing_data = {}
            with open(file_path, 'w') as json_file:
                json.dump(existing_data, json_file, indent=4)
        return existing_data

    except Exception as e:
        send_mail_or_logging('Error in loading json file', e)
        return {}

# Save JSON with flush/fsync
def save_json(file_path, data):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
            json_file.flush()
            os.fsync(json_file.fileno())
    except Exception as e:
        send_mail_or_logging('Error in saving data in json file', e)

# Add or update case for a machine
def add_case_to_json(case_entry: Dict[str, Any], machine_id: str, file_name: str):
    case_entry.setdefault("timestamp", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    file_path = os.path.join(CASE_FILE_PATH, file_name)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    data = load_json(file_path)

    if machine_id not in data:
        data[machine_id] = []

    # Replace if case_id exists
    for idx, entry in enumerate(data[machine_id]):
        if entry.get("case_id") == case_entry["case_id"]:
            data[machine_id][idx] = case_entry
            break
    else:
        data[machine_id].append(case_entry)

    save_json(file_path, data)
    logger.info(f"Upserted case under {machine_id} in {file_name}: {case_entry}")

# Method for send patient data to healthray lab
def send_to_healthray(results):
    try:
        url = LAB_URL

        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=45)
        payload = {
            'data': {
                'platform': 'Python',
                'branch_id': LAB_BRANCH_ID
            },
            'exp': expiry_time
        }
        jwt_token = jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')
        headers = {
            'authorization': f'{jwt_token}',
            'Content-Type': 'application/json'
        }

        output_dict = {
            'patient_case_results': [
                {
                    'lab_branch_id': LAB_BRANCH_ID,
                    'lis_machine_id': item[0],
                    'patient_case_no': item[1],
                    'result': item[2]
                }
                for item in results
            ]
        }

        #  REAL MODE
        # Uncomment to enable real API sending
        """
        response = requests.post(url, headers=headers, json=output_dict)

        if response is None:
            error_message = 'Error in API call - response is None'
            send_mail_or_logging(error_message, "None")
            return None

        if response.status_code == 200:
            logger.info("Successfully sent data to Healthray.")
            return True
        else:
            error_message = f'API call failed with status {response.status_code}'
            send_mail_or_logging(error_message, response.text)
            return None
        """

        #  BYPASS MODE (testing)
        for item in results:
            case_entry = {
                'lab_branch_id': LAB_BRANCH_ID,
                "lis_machine_id": item[0],
                "case_id": item[1],
                "result": item[2],
            }
            add_case_to_json(case_entry, item[0], 'sended_to_cloud.json')

        logger.info("Bypass mode: data written to local JSON successfully.")
        return True

    except Exception as e:
        error_message = 'Exception occurred in send_to_healthray'
        send_mail_or_logging(error_message, e)
        return None


threading.Thread(target=remove_old_logs_from_log_file, args=(LOG_FILE_LIST,), daemon=True).start()

while True:
    try:
        all_results = process_all_files(REPORT_FILE_PATH, BACKUP_FILE_PATH)
        if all_results:
            success = send_to_healthray(all_results)
            if success is None:
                logger.error("Healthray upload failed or skipped.")
            else:
                logger.info(f"Data sent to Healthray: {len(all_results)} cases")

            for result in all_results:
                logger.info(f"Processed case: {result}")
                print(f"Data Inserted :- {result}")
            time.sleep(2)

        time.sleep(8)
    except Exception as e:
        error_message = 'Error in main loop'
        send_mail_or_logging(error_message, e)
