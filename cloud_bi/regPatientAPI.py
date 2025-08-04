import os
import json
import time
import smtplib
import logging
import uvicorn # type: ignore
import requests
import threading
from fastapi import FastAPI, Query
from pydantic import BaseModel # type: ignore
from typing import List, Dict, Any
from email.message import EmailMessage
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse # type: ignore
from fastapi.middleware.cors import CORSMiddleware # type: ignore
import pymysql



# Initialize the FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],  
    allow_headers=["*"],  
    allow_credentials=False,  
)

HOST_ADDRESS = '192.168.1.160'
HOST_PORT = 5000

# Mysql Setup
MY_HOST = 'localhost'
MY_USER = 'root'
MY_PASS = 'root1'
MY_DB = 'lis'

# log file path
LOG_FILE_PATH = 'C:\\ASTM\\root\\log_file\\'
LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_register_patient.log'
ERROR_LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_register_patient_error.log'
LOG_FILE_LIST = [LOG_FILE, ERROR_LOG_FILE]

PAYLOAD_FILE = 'C:\\ASTM\\root\\case_file\\case_payload.json'
CASE_FILE = 'C:\\ASTM\\root\\case_file\\'
REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'


            
            
# Machine configuration

MACHINE_1_ID = 'C_311_1'
MACHINE_1_COM_PORT = 'COM1'
MACHINE_1_HOST_ADDRESS = '127.0.0.1'
MACHINE_1_HOST_PORT = 6010

MACHINE_2_ID = 'E_411_1'
MACHINE_2_COM_PORT = 'COM2'
MACHINE_2_HOST_ADDRESS = '127.0.0.1'
MACHINE_2_HOST_PORT = 6020

MACHINE_3_ID = 'ACCESS_2'
MACHINE_3_COM_PORT = 'COM3'
MACHINE_3_HOST_ADDRESS = '127.0.0.1'
MACHINE_3_HOST_PORT = 6030

MACHINE_4_ID = 'MISPA_NANO'
MACHINE_4_COM_PORT = 'P5040'
MACHINE_4_HOST_ADDRESS = '192.168.1.160'
MACHINE_4_HOST_PORT = 6040

MACHINE_5_ID = 'BS_240'
MACHINE_5_COM_PORT = 'P5050'
MACHINE_5_HOST_ADDRESS = '192.168.1.160'
MACHINE_5_HOST_PORT = 6050


SAMPLE_TYPE_MAPPING_FOR_MACHINE = {
    "serum": "S1",
    "plasma":  "S1",
    "urine": "S2",
    "sodium flouride": "S3",
    "suprnt": "S4",
    "others": "S5",
    "edta whole blood": "S1",
    "edta": "S1",
    "whole blood": "S1",
    "blood": "S1",
    "whole blood": "S1",
    "plain serum": "S1"
}

# Email configuration
SUBJECT = ''
TO_EMAIL = ""
FROM_EMAIL = ""
PASSWORD = "" 

# Pydantic model for request body validation
class Case(BaseModel):
    case_id: str
    test_id: list
    machine_id: str
    sample_type: str
    patient_name: str
    gender: str

class CreateCaseRequest(BaseModel):
    cases: List[Case]

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
        error_message = 'Error in remove old log'
        # send_mail_or_logging(error_message, e)
        return logger

logger = setup_loggers(LOG_FILE, ERROR_LOG_FILE)
logger.info('Log file initialized.')
logger.error('This is an error log example.')

# Methid for sending email and logging
def send_mail_or_logging(error_message, error):
    logger.error(f'{error_message} : {error}')

    body = f"""
    Dear Team,

    This is an automated alert from the Laboratory Information System.

        --> {error_message} :: Below are the details of the incident:

        - Filename: register_patient.py
        - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        - Error Details: {error}

    Best regards,  
    Laboratory Information System  
    [hlray LAB]
    """

    # is_send_mail = send_email(SUBJECT, body, TO_EMAIL, FROM_EMAIL, PASSWORD)
    # if not is_send_mail:
        # logger.error(f"Exception during sending mail")

def remove_old_logs_from_log_file(log_file_list):
    try:
        logger.info("Thread Start For Remove old Logs")
        while True:
            for file in log_file_list:
                with open(file, 'r') as f:  
                    data = f.read()

                lines = data.split('\n')

                cutoff_date = datetime.now() - timedelta(minutes=10)
                final_line_list = []
                for line in lines:
                    line_date = line.split()
                    if len(line_date) > 1:
                        if line_date[0] > (str(cutoff_date)).split()[0]:
                            final_line_list.append(line)

                with open(file,'w') as f:
                    f.write('\n'.join(final_line_list))
            
            time.sleep(86400)
    except Exception as e:
        error_message = 'Error in remove old log'
        send_mail_or_logging(error_message, e)

def process_case_machine_wise(machine_host_address, machine_host_port, machine_com_port, case, case_id, sample_type, machine_id, responses):
    try:
        response = requests.post(f"http://{machine_host_address}:{machine_host_port}/create_case/{machine_com_port}", json=case)
        case_entry = {
            "status": "success",
            "case_id": case_id,
            "sample_type": sample_type,
            "response": response.status_code,
            "api": f"http://{machine_host_address}:{machine_host_port}/create_case/{machine_com_port}",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        add_case_to_json(case_entry, machine_id, 'case_creation_api.json')
        if response.status_code == 200:
            responses.append({
                "status": 200,
                "statusState": "success",
                "message": f"Case Creation Process Started for machine {machine_id}",
                "case_id": case_id,
            })
        else:
            case_entry = {
                "status": "fail",
                "case_id": case_id,
                "sample_type": sample_type,
                "response": response.status_code,
                "api": f"http://{machine_host_address}:{machine_host_port}/create_case/{machine_com_port}",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            add_case_to_json(case_entry, machine_id, 'case_creation_api_fail.json')
            responses.append({
                "status": 400,
                "statusState": "error",
                "case_id": case_id,
                "message": f"Failed to create case {case_id} for machine_id {machine_id}. Status code: {response.status_code}",
            })

            error_message = f"Failed to create case {case_id} for machine_id {machine_id}. Status code: {response.status_code}"
            error = 'Failed to create case'
            send_mail_or_logging(error_message, error)
        
        return responses

    except requests.exceptions.RequestException as e:
        responses.append({
            "status": 400,
            "statusState": "error",
            "case_id": case_id,
            "message": f"Request to create case {case_id} failed: {str(e)}",
        })

        error_message = f"Request to create case {case_id} failed: {str(e)}"
        send_mail_or_logging(error_message, e)
        
        return responses

def handle_machine(case, case_id, machine_id, responses, formatted_test_id, sample_type_mapping_for_machine, machine_host_address, machine_host_port, machine_com_port):
    sample_type = case.get("sample_type", "").lower()
    for key, value in sample_type_mapping_for_machine.items():
        if key in sample_type:
            case["sample_type"] = value
            break

    case["test_id"] = formatted_test_id

    process_case_machine_wise(machine_host_address, machine_host_port, machine_com_port, case, case_id, sample_type, machine_id, responses)

# Function to process a single case
def process_case(case: Dict[str, Any], case_id: str, responses: List[Dict[str, Any]]):
    try:
        test_id = case.get("test_id")
        machine_id = case.get("machine_id")

        if not all([case_id, test_id, machine_id]):
            responses.append({
                "status": 400,
                "statusState": "error",
                "message": f"Missing required data for case_id {case_id}. Expected: test_id, machine_id."
            })
            error_message = 'Error in remove old log'
            send_mail_or_logging(error_message, e)
            return

        # Handle sample type and reformat test_id
        if machine_id == MACHINE_1_ID:
            formatted_test_id = "^^^" + "^/^^^".join(test_id) + "^"
            handle_machine(case, case_id, machine_id, responses, formatted_test_id, SAMPLE_TYPE_MAPPING_FOR_MACHINE, MACHINE_1_HOST_ADDRESS, MACHINE_1_HOST_PORT, MACHINE_1_COM_PORT)

        elif machine_id == MACHINE_2_ID:
            formatted_test_id = "^^^" + "^/^^^".join(test_id) + "^"

            handle_machine(case, case_id, machine_id, responses, formatted_test_id, SAMPLE_TYPE_MAPPING_FOR_MACHINE, MACHINE_2_HOST_ADDRESS, MACHINE_2_HOST_PORT, MACHINE_2_COM_PORT)
 
        elif machine_id == MACHINE_3_ID:
            formatted_test_id = "\\".join(test_id)

            handle_machine(case, case_id, machine_id, responses, formatted_test_id, SAMPLE_TYPE_MAPPING_FOR_MACHINE, MACHINE_3_HOST_ADDRESS, MACHINE_3_HOST_PORT, MACHINE_3_COM_PORT)
 
        elif machine_id == MACHINE_4_ID:
            formatted_test_id = "\\".join(test_id)

            handle_machine(case, case_id, machine_id, responses, formatted_test_id, SAMPLE_TYPE_MAPPING_FOR_MACHINE, MACHINE_4_HOST_ADDRESS, MACHINE_4_HOST_PORT, MACHINE_4_COM_PORT)
 
        elif machine_id == MACHINE_5_ID:
            formatted_test_id = '\\'.join(f"{i+1}^{test_id}^2^1" for i, test_id in enumerate(test_id))

            handle_machine(case, case_id, machine_id, responses, formatted_test_id, SAMPLE_TYPE_MAPPING_FOR_MACHINE, MACHINE_5_HOST_ADDRESS, MACHINE_5_HOST_PORT, MACHINE_5_COM_PORT)
 
        else:
            responses.append({
                "status": 400,
                "statusState": "error",
                "case_id": case_id,
                "message": f"Invalid machine_id {machine_id} for case_id {case_id}",
            })

            error_message = 'Error in create case'
            error = 'Machine id not found'
            send_mail_or_logging(error_message, error)

    except Exception as e:
        responses.append({
            "status": 400,
            "statusState": "error",
            "case_id": case_id,
            "message": f"Unexpected error while processing case {case_id}: {str(e)}",
        })
        error_message = 'Error in process case'
        send_mail_or_logging(error_message, e)



# def add_case_to_json(case_entry, machine_id, file_name):
#     try:
#         file_path = os.path.join('C:\\ASTM\\root\\case_file\\', file_name)
#         os.makedirs(os.path.dirname(file_path), exist_ok=True)

#         # Convert Pydantic object to dict if needed
#         if hasattr(case_entry, "dict"):
#             case_entry = case_entry.dict()

#         # Load existing data or initialize new
#         if os.path.exists(file_path):
#             with open(file_path, 'r') as json_file:
#                 try:
#                     existing_data = json.load(json_file)
#                     if not isinstance(existing_data, dict):
#                         print("⚠️ Existing JSON is not a dict, resetting.")
#                         logger.warning("Existing JSON is not a dict, resetting.")
#                         existing_data = {}
#                 except json.JSONDecodeError:
#                     print("⚠️ JSON decode error, resetting file.")
#                     logger.warning("JSON decode error, resetting file.")
#                     existing_data = {}
#         else:
#             print(f"⚠️ File not found: {file_path}, creating new.")
#             logger.warning(f"File not found: {file_path}, creating new.")
#             existing_data = {}

#         # Filter entries older than 10 days
#         if machine_id not in existing_data:
#             existing_data[machine_id] = []

#         cutoff_date = datetime.now() - timedelta(days=10)
#         existing_data[machine_id] = [
#             entry for entry in existing_data[machine_id]
#             if datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff_date
#         ]

#         # Overwrite if case_id exists, otherwise append
#         found = False
#         for idx, entry in enumerate(existing_data[machine_id]):
#             if entry["case_id"] == case_entry["case_id"]:
#                 existing_data[machine_id][idx] = case_entry
#                 found = True
#                 break

#         if not found:
#             existing_data[machine_id].append(case_entry)

#         # Save to file
#         with open(file_path, 'w') as json_file:
#             json.dump(existing_data, json_file, indent=4)
#             json_file.flush()
#             os.fsync(json_file.fileno())

#         print(f"✅ Saved case to JSON: {case_entry}")
#         logger.info(f"Saved case to JSON: {case_entry}")

#     except Exception as e:
#         print(f"❌ Error saving case to JSON: {e}")
#         logger.error(f"Error saving case to JSON: {e}")
#         send_mail_or_logging('Error in add_case_to_json', e)


# Load JSON with machine_id as top-level keys
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

# Save JSON to file
def save_json(file_path, data):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
            json_file.flush()
            os.fsync(json_file.fileno())
    except Exception as e:
        send_mail_or_logging('Error in saving data in json file', e)

# Add or update a case in machine_id-specific list
def add_case_to_json(case_entry: Dict[str, Any], machine_id: str, file_name: str):

    # Ensure fields
    case_entry.setdefault("timestamp", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    case_entry.setdefault("machine_id", machine_id)

    file_path = os.path.join(CASE_FILE, file_name)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    data = load_json(file_path)

    if machine_id not in data:
        data[machine_id] = []

    # Upsert case by case_id
    for idx, entry in enumerate(data[machine_id]):
        if entry.get("case_id") == case_entry["case_id"]:
            data[machine_id][idx] = case_entry
            break
    else:
        data[machine_id].append(case_entry)

    save_json(file_path, data)
    logger.info(f"Upserted case for {machine_id} in {file_name}: {case_entry}")

# Background thread to remove entries older than N days
def remove_old_case_entries_from_files(json_file_list: List[str], days: int = 10, sleep_interval: int = 86400):

    try:
        logger.info("Thread start: remove_old_case_entries_from_files")
        while True:
            cutoff = datetime.now() - timedelta(days=days)

            for file_path in json_file_list:
                try:
                    data = load_json(file_path)
                    changed = False

                    for machine_id in list(data.keys()):
                        if not isinstance(data[machine_id], list):
                            continue

                        kept = []
                        for entry in data[machine_id]:
                            try:
                                ts = datetime.strptime(entry["timestamp"], '%Y-%m-%d %H:%M:%S')
                                if ts > cutoff:
                                    kept.append(entry)
                            except (KeyError, ValueError):
                                continue

                        if len(kept) != len(data[machine_id]):
                            data[machine_id] = kept
                            changed = True

                    if changed:
                        save_json(file_path, data)
                        logger.info(f"Pruned {file_path}")

                except Exception as inner:
                    send_mail_or_logging(f"Error pruning {file_path}", inner)

            # time.sleep(sleep_interval)
            time.sleep(3)

    except Exception as e:
        send_mail_or_logging("Fatal error in remove_old_case_entries_from_files", e)

def add_new_payload_entry(case_data, payload_file):
    try:
        payload_data = load_json(payload_file)

        for case_data_item in case_data:
            case_dict = case_data_item.__dict__
            machine_id = case_dict.get("machine_id", "UNKNOWN_MACHINE")
            case_dict["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if machine_id not in payload_data:
                payload_data[machine_id] = []

            payload_data[machine_id].append(case_dict)

        save_json(payload_file, payload_data)

    except Exception as e:
        send_mail_or_logging('Error in add new payload entry', e)


def remove_old_payload_entry(payload_file):
    try:
        while True:
            payload_file_data = load_json(payload_file)
            cutoff_date = datetime.now() - timedelta(minutes=10)
            changed = False

            for machine_id in list(payload_file_data.keys()):
                kept = []
                for case in payload_file_data[machine_id]:
                    try:
                        ts = datetime.strptime(case["timestamp"], '%Y-%m-%d %H:%M:%S')
                        if ts > cutoff_date:
                            kept.append(case)
                    except Exception:
                        continue

                if len(kept) != len(payload_file_data[machine_id]):
                    payload_file_data[machine_id] = kept
                    changed = True

            if changed:
                save_json(payload_file, payload_file_data)

            time.sleep(2)

    except Exception as e:
        send_mail_or_logging('Error in remove old payload entry', e)


# API endpoint to create cases
@app.post("/create_case")
async def api_create_case(request: CreateCaseRequest):
    payload_data = []
    try:
        data = request.cases  # List of cases
        payload_data.append(data)
        responses = []
        
        
        if not data:
            error_message = 'Error in create case api calling'
            error = 'No data found'
            send_mail_or_logging(error_message, error)
            return JSONResponse(
                content={
                    "status": 400,
                    "statusState": "error",
                    "data": responses,
                    "message": "No data provided",
                }
            )
        
        add_new_payload_entry(data, PAYLOAD_FILE)
        
        pending_cases = {}
        threads = []
        for case in data:
            case_id = case.case_id
            machine_id = case.machine_id
            if not case_id:
                responses.append({
                    "status": 400,
                    "statusState": "error",
                    "message": "Missing case_id in the request",
                })
                continue
            case_entry = {
                "case_id": case_id,
                "api": f"http://{HOST_ADDRESS}:{HOST_PORT}/create_case",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            add_case_to_json(case_entry, machine_id, 'log_main_api.json')
            thread = threading.Thread(target=process_case, args=(case.model_dump(), case_id, responses))
            pending_cases[case_id] = thread
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        return JSONResponse(
            content={
                "status": 200,
                "statusState": "success",
                "data": responses,
                "message": "Process Started",
            }
        )
    except Exception as e:
        case_entry = {
            "api": f"http://{HOST_ADDRESS}:{HOST_PORT}/create_case",
            "payload_data": payload_data,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        add_case_to_json(case_entry, "LAB Machine", 'log_main_api_fail.json')

        error_message = 'Error in create case api calling'
        send_mail_or_logging(error_message, e)

        return JSONResponse(
            content={
                "status": 400,
                "statusState": "error",
                "data": [],
                "message": f"Internal Threading Error : {e}",
            }
        )


# API endpoint to get patient data
@app.get("/get_patient_data")
async def get_patient_data(pid: str = Query(None)):
    try:
        if pid:
            conn = pymysql.connect(
                host=MY_HOST,
                user=MY_USER,
                password=MY_PASS,
                database=MY_DB,
                cursorclass=pymysql.cursors.DictCursor  # Using DictCursor for returning results as dictionaries
            )
            cursor = conn.cursor()
            query = f"SELECT * FROM machine_data WHERE patientId = '{pid}'"
            cursor.execute(query)
            result = cursor.fetchall()

            cursor.close()
            conn.close()

            if result:
                response = {
                    "data": result,
                    "message": "LIS results retrieved successfully",
                    "statusState": "success",
                    "status": 200
                }
            
            else:
                response = {
                    "data": [],
                    "message": "Report Not Ready",
                    "statusState": "success",
                    "status": 200
                }
        else:
            # Case when pid is not provided
            response = {
                "data": [],
                "message": "Case ID Not Found",
                "statusState": "fail",
                "status": 422
            }

        return JSONResponse(content=response)

    except Exception as e:
        response = {
            "data": [],
            "message": f"Internal Server Error- {e}",
            "statusState": "error",
            "status": 500
        }

        error_message = 'Error in get patient data api call'
        send_mail_or_logging(error_message, e)

        return JSONResponse(content=response)



# Run the app with Uvicorn if executed directly
if __name__ == "__main__":
    try:
        
        threading.Thread(target=remove_old_logs_from_log_file, args=(LOG_FILE_LIST,), daemon=True).start()
        threading.Thread(target=remove_old_payload_entry, args=(PAYLOAD_FILE,), daemon=True).start()
        JSON_FILE_LIST = [
            os.path.join(CASE_FILE, 'case_creation_api.json'),
            os.path.join(CASE_FILE, 'case_creation_api_fail.json'),
            os.path.join(CASE_FILE, 'log_main_api.json'),
            os.path.join(CASE_FILE, 'log_main_api_fail.json'),
        ]
        threading.Thread(target=remove_old_case_entries_from_files, args=(JSON_FILE_LIST, 10, 86400), daemon=True ).start()

        uvicorn.run(app, host=HOST_ADDRESS, port=HOST_PORT)
    except Exception as e:
        error_message = 'Error in main function'
        send_mail_or_logging(error_message, e)