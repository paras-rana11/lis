# handling create_case-in_machine fails then add that case entry to db and retry after 10 mins
# SET 60 TO 600 BAKI



# ==============================================================================
# ✅ 0. Imports
# ==============================================================================
import os
import sys
import time
import json
import serial  
import uvicorn
import logging
import smtplib
import threading
import mysql.connector
import mysql.connector.pooling
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime, timedelta
from email.message import EmailMessage
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, BackgroundTasks
from concurrent.futures import ThreadPoolExecutor, TimeoutError



# ✅ 1. Constants
# Machine Configuration
MACHINE_NAME = 'Cobas C 311 1'
HOST_ADDRESS = '127.0.0.1'
HOST_PORT = 6010
HOST_COM_PORT = 'COM1'
MACHINE_ID = 'C_311_2'
REPORT_FILE_PREFIX = 'c_311_2_'
CONNECTION_TYPE = 'serial'


# Global objects and variables
LOCK =  threading.Lock()
FLAG_LOCK = threading.Lock()


# Flags controlling thread behavior
IS_RECIEVING_DATA = False
IS_UNIDIRECTIONAL_RUNNING = False
IS_BIDIRECTIONAL_RUNNING = False
IS_PENDING_RETRY_RUNNING = False


LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_c_311_2.log'
ERROR_LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_c_311_2_error.log'
PENDING_CASE_LOG_FILE = 'C:\\ASTM\\root\\log_file\\pending_cases_log.log'
LOG_FILE_LIST = [LOG_FILE, ERROR_LOG_FILE, PENDING_CASE_LOG_FILE]

log_check_interval = 86400  # 24 hours (in seconds)

SUBJECT = "Email From SGL Hospital"
TO_EMAIL = "lishealthray@gmail.com"                            # NOTE: to change
FROM_EMAIL = "lishealthray@gmail.com"
PASSWORD = "rxbr zlzy tpin pgur" 

REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'
CASE_FILE_PATH = 'C:\\ASTM\\root\\case_file\\'
LOG_FILE_PATH = 'C:\\ASTM\\root\\log_file\\'



# ✅ 2. Logging Setup
class PendingCaseFilter(logging.Filter):
    def filter(self, record):
        return "[PENDING]" in record.getMessage()

def setup_loggers(log_file, error_log_file, pending_case_log_file):
    '''Set up separate loggers for info and error.'''
    logger = logging.getLogger('app_logger')
    try:
        logger.setLevel(logging.DEBUG)  # capture everything, filter in handlers
        logger.handlers.clear()  # avoid duplicate logs on reload

        # Info Handler
        info_handler = logging.FileHandler(log_file, encoding='utf-8')
        info_handler.setLevel(logging.INFO)
        info_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        info_handler.setFormatter(info_format)

        # Error Handler
        error_handler = logging.FileHandler(error_log_file, encoding='utf-8')
        error_handler.setLevel(logging.ERROR)
        error_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        error_handler.setFormatter(error_format)
        
        # Pending Case Handler
        pending_handler = logging.FileHandler(pending_case_log_file, encoding='utf-8')
        pending_handler.setLevel(logging.INFO)
        pending_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        pending_handler.setFormatter(pending_format)
        # Apply custom filter
        pending_handler.addFilter(PendingCaseFilter())

        logger.addHandler(info_handler)
        logger.addHandler(error_handler)
        logger.addHandler(pending_handler)

        return logger
    except Exception as e:
        print(f"Error in initialize logger : {e}")
        return logger

logger = setup_loggers(LOG_FILE, ERROR_LOG_FILE, PENDING_CASE_LOG_FILE)
logger.info('Log file initialized.')
logger.error('This is an error log example.')

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
    
def send_mail_or_logging(error_message, error):
    logger.error(f'{error_message} : {error}')

    body = f"""
    Dear Team,

    This is an automated alert from the Laboratory Information System.

        --> {error_message} :: Below are the details of the incident:

        - Filename: cobas_c_311_2.py
        - Connetion Type: {CONNECTION_TYPE}
        - Machine Name: {MACHINE_NAME}
        - Machine Host Address: {HOST_ADDRESS}
        - Port: {HOST_PORT}
        - COM Port: {HOST_COM_PORT}
        - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        - Error Details: {error}

    Best regards,  
    Laboratory Information System  
    [Healthray LAB]                     
    """

    is_send_mail = send_email(SUBJECT, body, TO_EMAIL, FROM_EMAIL, PASSWORD)
    if not is_send_mail:
        logger.error(f"Exception during sending mail")

def remove_old_logs_from_log_file(log_file_list):
    try:
        logger.info("Thread Start For Remove old Logs")
        while True:
            for file in log_file_list:
                with open(file, 'r', encoding="utf-8", errors="ignore") as f:
                    data = f.read()

                lines = data.split('\n')

                cutoff_date = datetime.now() - timedelta(days=10)
                final_line_list = []
                for line in lines:
                    line_date = line.split()
                    if len(line_date) > 1:
                        if line_date[0] > (str(cutoff_date)).split()[0]:
                            final_line_list.append(line)

                with open(file,'w', encoding="utf-8") as f:
                    f.write('\n'.join(final_line_list))
            
            time.sleep(86400)
    except Exception as e:
        error_message = 'Error in remove old log'
        send_mail_or_logging(error_message, e)



# ✅ 3. Utility Classes
class GenerateChecksum:
    def __init__(self):
        # Initialize ASTM Protocol Constants
        self.STX = chr(0x02)  # Start of text
        self.ETX = chr(0x03)  # End of text
        self.EOT = chr(0x04)  # End of transmission
        self.ENQ = chr(0x05)  # Enquiry
        self.ACK = chr(0x06)  # Acknowledge
        self.NAK = chr(0x15)  # Negative Acknowledge
        self.CR = chr(0x0D)   # Carriage Return
        self.LF = chr(0x0A)   # Line Feed

    def get_checksum_value(self, frame: str) -> str:
        """
        Reads checksum of an ASTM frame. Calculates characters after STX,
        up to and including the ETX or ETB.
        """
        sum_of_chars = 0
        complete = False

        # Loop through each character in the frame to calculate checksum
        for char in frame:
            byte_val = ord(char)

            if byte_val == 2:
                sum_of_chars = 0  
            elif byte_val in {3, 23}:
                sum_of_chars += byte_val
                complete = True
                break
            else:
                sum_of_chars += byte_val

        # Return the checksum if complete and valid
        if complete and sum_of_chars > 0:
            checksum = hex(sum_of_chars % 256)[2:].upper()  
            return checksum.zfill(2)  
        return "00" 

generate_checksum = GenerateChecksum()
   
      
class MachineConnectionSerial:
    def __init__(self, connection_type: str, input_tty: str, machine_name: str):
        self.connection_type = connection_type
        self.input_tty = input_tty
        self.machine_name = machine_name
        self.port = None

    def get_filename(self):
        dt = datetime.datetime.now()
        return REPORT_FILE_PATH + self.machine_name + dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

    def get_port(self):
        try:
            self.port = serial.Serial(port=self.input_tty, baudrate=9600, timeout=1)
            print(f"Serial port {self.input_tty} opened successfully")
            return self.port, CASE_FILE_PATH
        except serial.SerialException as se:
            print(f"Failed to open serial port {self.input_tty}: {se}")
            sys.exit(1)

    def read(self):
        if not self.port:
            raise Exception("Port is not initialized. Call get_port() first.")
        data = self.port.read(1)
        # print(f"Received: {data}")
        return data

    def write(self, byte):
        if not self.port:
            raise Exception("Port is not initialized. Call get_port() first.")
        self.port.write(byte)
        print(f"Query Sent: {byte}")

# Try to establish connection
try:
    connection_c_311 = MachineConnectionSerial(
        connection_type=CONNECTION_TYPE,
        input_tty=HOST_COM_PORT, 
        machine_name=REPORT_FILE_PREFIX
    )
    
    port_c_311, path = connection_c_311.get_port()
    logger.info(f"Connection established on port: {port_c_311}")
except Exception as e:
    error_message = 'Failed to establish machine connection'
    send_mail_or_logging(error_message, e)


class DBConnectionPool:
    def __init__(self, host, user, password, database, pool_name="lis_pool", pool_size=10):
        try:
            self.pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                pool_reset_session=True,
                host=host,
                user=user,
                password=password,
                database=database
            )
            logger.info("[DB] ✅ Connection pool created.")
        except Exception as e:
            logger.exception("[DB] ❌ Failed to create connection pool:")
            self.pool = None

    def get_connection(self, retries=3, delay=0.5):
        for attempt in range(1, retries + 1):
            try:
                return self.pool.get_connection()
            except Exception as e:
                if "pool exhausted" in str(e).lower():
                    logger.warning(f"[DB] Pool exhausted. Retry {attempt}/{retries} after {delay} sec...")
                    time.sleep(delay)
                else:
                    logger.exception("[DB] Unexpected error while getting connection:")
                    raise
        logger.error("[DB] ❌ Could not get DB connection after retries.")
        return None

db_pool = DBConnectionPool(
    host="localhost",
    user="root",
    password="root1",
    database="lis",
    pool_size=10
)



# ✅ 4. JSON File Handling
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

#  Prune old cases (per machine_id)
def remove_old_case_entries_from_json_files(json_file_list: List[str], days: int = 10, sleep_interval: int = 86400):
    try:
        logger.info("Thread start: remove_old_case_entries_from_json_files")
        while True:
            cutoff = datetime.now() - timedelta(days=days)

            for file_path in json_file_list:
                try:
                    data = load_json(file_path)
                    changed = False

                    for machine_id in list(data.keys()):
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

            time.sleep(sleep_interval)

    except Exception as e:
        send_mail_or_logging("Fatal error in remove_old_case_entries_from_json_files", e)



# ✅ 5. MySQL Pending Case Handling
# if we update record mannually then this func is needed. 
def is_case_in_pending_cases(case_no):
    """Check if the case is already in the pending DB."""
    try:
        conn = db_pool.get_connection()
        if not conn:
            logger.error("[PENDING] DB connection not available in is_case_in_pending_cases")
            return False
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM pending_cases WHERE case_id = %s AND machine_id = %s"
        cursor.execute(query, (case_no, MACHINE_ID))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        send_mail_or_logging("[PENDING] Error in is_case_in_pending_cases", e)
        return False  

def is_case_in_json(case_id, machine_id, filename):
    """Check if case already exists in a specific JSON file."""
    data = load_json(os.path.join(CASE_FILE_PATH, filename))
    for entry in data.get(machine_id, []):
        if entry.get("case_id") == case_id:
            return True
    return False

def insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, machine_id):
     
    try:
        conn = db_pool.get_connection()
        if not conn:
            logger.error("[PENDING] DB connection not available in insert_pending_case_to_db")
            return
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO pending_cases (case_id, test_id, sample_type, sample_type_number, machine_id)
            VALUES (%s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                test_id = VALUES(test_id),
                sample_type = VALUES(sample_type),
                sample_type_number = VALUES(sample_type_number);
        """
        values = (case_id, test_id, sample_type, sample_type_number, machine_id)
        cursor.execute(insert_query, values)
        conn.commit()
        cursor.close()
        conn.close()
        case_entry = {
            "case_id": case_id,
            "test_id": test_id,
            "sample_type": sample_type,
            "sample_type_number": sample_type_number,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        logger.info(f"[PENDING] Upserted to pending_cases: {case_id}, machine_id={machine_id} || Values: {case_entry}")
    except Exception as e:
        send_mail_or_logging("[PENDING] Error inserting into pending_cases", e)
        
def delete_pending_case(case_id_db):
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        del_query = "DELETE FROM pending_cases WHERE case_id = %s AND machine_id = %s"
        cursor.execute(del_query, (case_id_db, MACHINE_ID))
        conn.commit()
        if cursor.rowcount == 0:
            logger.warning(f"[PENDING] No record deleted for case_id={case_id_db} and machine_id={MACHINE_ID}")
        else:
            logger.info(f"[PENDING] Case ID {case_id_db} deleted from pending_cases table for machine {MACHINE_ID}. 🗑️")
        cursor.close()
    except Exception as e:
        logger.error(f"[PENDING] Failed to delete case ID {case_id_db} for machine {MACHINE_ID}. ⚠️")
        send_mail_or_logging(f"[PENDING] DB deletion error for case ID {case_id_db} on machine {MACHINE_ID}", e)
    finally:
        if conn:
            conn.close()

RETRY_IN_PROGRESS = set()

def check_and_retry_pending_cases(connection):
    global IS_PENDING_RETRY_RUNNING, IS_RECIEVING_DATA
  
    with FLAG_LOCK:
        if IS_PENDING_RETRY_RUNNING:
            logger.info("[PENDING] Retry already running, skipping.")
            return False
        if IS_BIDIRECTIONAL_RUNNING or IS_UNIDIRECTIONAL_RUNNING:
            logger.info("[PENDING] Skipping retry — BI or UNI is active. ⏸️")
            IS_RECIEVING_DATA = True  # Tell uni-reader to pause
            return False
        
        IS_RECIEVING_DATA = False
        IS_PENDING_RETRY_RUNNING = True
            
    try: 
        logger.info(f"[PENDING] {'=' * 100}")
        logger.info(f"[PENDING] 🕒 Starting new retry cycle")
        
        conn = db_pool.get_connection()
        if conn is None:
            logger.warning("[PENDING] ⚠ DB connection failed. Will retry in 1 minute.")
            time.sleep(60)
            return False
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM pending_cases where machine_id = %s", (MACHINE_ID,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        
        if not rows:
            logger.info("[PENDING]  No pending cases found. ")
            return False
        
        logger.info(f"[PENDING] {'=' * 100}")
        logger.info(f"[PENDING] 📋 Found {len(rows)} pending cases.")

        for row in rows:
            logger.info("[PENDING] " +  "="*80)
            logger.info(f"[PENDING] Processing MACHINE_ID: '{row['machine_id']}', DATA: {row}")
            case_id = row['case_id']
            with FLAG_LOCK:
                if case_id in RETRY_IN_PROGRESS:
                    logger.info(f"[PENDING] Skipping case {case_id} — already being retried.")
                    continue
                RETRY_IN_PROGRESS.add(case_id)
            
            try:
                
                logger.info(f"[PENDING] Retrying case: {row['case_id']}")
                case_entry = {
                    "case_id": row["case_id"],
                    "test_id": row["test_id"],
                    "sample_type": row["sample_type"],
                    "sample_type_number": row["sample_type_number"],
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                response = create_case_in_machine(
                    connection,
                    case_no=row["case_id"],
                    test_id=row["test_id"],
                    sample_type=row["sample_type"],
                    sample_type_number=row["sample_type_number"]
                )

                if response.get("status") == "success":
                    add_case_to_json(case_entry, row["machine_id"], 'complete_case_creation.json')
                    delete_pending_case(row["case_id"])
                    logger.info(f"[PENDING] Case {row['case_id']} successfully processed and removed from DB. ✔️")
                else:
                    add_case_to_json(case_entry, row["machine_id"], 'error_case_creation.json')
                    logger.warning(f"[PENDING] Pending Retry failed for Case {row['case_id']}: {response.get('message')}. ❌")

            except Exception as e:
                send_mail_or_logging(f"[PENDING] Error processing case {row['case_id']}.⚠️", e)
            finally:
                with FLAG_LOCK:
                    RETRY_IN_PROGRESS.discard(case_id)
        return True
    except Exception as e:
        logger.exception(f"[PENDING] Exception occurred during retry cycle: {e}. ⚠️")
        send_mail_or_logging("[PENDING] Error in retry pending case thread", e)
        
    finally:
        with FLAG_LOCK:
            IS_PENDING_RETRY_RUNNING = False
        logger.info("[PENDING] " + "=" * 80)


# ✅ 6. Machine Communication Core - BIDIRECTION
def send_message(connection, message):
    """Send a framed message with checksum to the machine."""
    try:
        framed_message = f"{generate_checksum.STX}{message}{generate_checksum.CR}{generate_checksum.ETX}"
        checksum = generate_checksum.get_checksum_value(framed_message)
        final_message = f"{framed_message}{checksum}{generate_checksum.CR}{generate_checksum.LF}"
        connection.write(final_message.encode())
    except Exception as e:
        error_message = 'Error in send msg function'
        send_mail_or_logging(error_message, e)

def create_case_in_machine1(connection, case_no, test_id, sample_type, sample_type_number):
    """Send commands to the machine to create a case."""
    
    with LOCK:
        try:
            connection.write(generate_checksum.ENQ.encode())
            byte = connection.read()
            logger.info(f"[BI] Read byte (create_case_in_machine) : {byte}")

            if byte == generate_checksum.ACK.encode():
                # Step 1: Send header message
                header_message = f"1H|\\^&|||host^1|||||C311|TSDWN^BATCH|P|1"
                send_message(connection, header_message)
                if connection.read() != generate_checksum.ACK.encode():
                    return {"status": "error", "message": "Header Message Not Acknowledged"}
                logger.info(f"HEADER MESSAGE :-       {header_message}")

                # Step 2: Send patient message
                patient_message = f"2P|1"
                send_message(connection, patient_message)
                if connection.read() != generate_checksum.ACK.encode():
                    return {"status": "error", "message": "Patient Message Not Acknowledged"}
                logger.info(f"PATIENT MESSAGE :-      {patient_message}")

                # Step 3: Send order message
                order_message = (
                    f"3O|1|{' ' * (22 - len(case_no))}{case_no}|^^^^{sample_type}^SC|{test_id}|R||||||A||||{sample_type_number}||||||||||O"
                )
                send_message(connection, order_message)
                if connection.read() != generate_checksum.ACK.encode():
                    return {"status": "error", "message": "Order Message Not Acknowledged"}
                logger.info(f"ORDER MESSAGE :-        {order_message}")

                # Step 4: Send comment message
                comment_message = (
                    f"4C|1|I|                              ^                         ^                    ^               ^          |G"
                )
                send_message(connection, comment_message)
                if connection.read() != generate_checksum.ACK.encode():
                    return {"status": "error", "message": "Comment Message Not Acknowledged"}
                logger.info(f"COMMENT MESSAGE :-      {comment_message}")

                # Step 5: Send termination message
                termination_message = f"5L|1|N"
                send_message(connection, termination_message)
                if connection.read() == generate_checksum.ACK.encode():
                    connection.write(generate_checksum.EOT.encode())
                    logger.info(f"TERMINATION MESSAGE :-  {termination_message}")
                    logger.info("*****************************************************")
                    logger.info(" ")
                    return {"status": "success", "message": "Case created successfully"}
                else:
                    return {"status": "error", "message": "Termination Message Not Acknowledged"}
            else:
                return {"status": "error", "message": "ENQ Message Not Acknowledged"}
            
        except Exception as e:
            error_message = f'[PENDING] Error in create case in machine, Case id: {case_no}'
            send_mail_or_logging(error_message, e)
            return {"status": "error", "message": str(e)}


import random
def create_case_in_machine(connection, case_no, test_id, sample_type, sample_type_number):
    """Send commands to the machine to create a case."""
    
    with LOCK:
        try:
            choice = random.choice(["success", "error", "exception"])
            
            if choice == "success":
                # logger.info(f"[BI-TEST] Simulating SUCCESS for case {case_no}")
                return {"status": "success", "message": "Case created successfully "}
            
            elif choice == "error":
                # logger.info(f"[BI-TEST] Simulating FAILURE for case {case_no}")
                return {"status": "error", "message": "Faled"}
            
            else:  # exception
                # logger.info(f"[BI-TEST] Simulating EXCEPTION for case {case_no}")
                raise Exception("exception during case creation")
            
            # Original code commented out for now...
            
        except Exception as e:
            error_message = f'[PENDING] Error in create case in machine, Case id: {case_no}'
            send_mail_or_logging(error_message, e)
            return {"status": "error", "message": str(e)}



# ✅ 7. Retry Mechanism
def retry_api_call(connection, case_id, test_id, sample_type, machine_id):
    
    """Retry API call to ensure the case is created."""
    global IS_BIDIRECTIONAL_RUNNING, IS_UNIDIRECTIONAL_RUNNING, IS_PENDING_RETRY_RUNNING
    
    with FLAG_LOCK:
        IS_BIDIRECTIONAL_RUNNING = True
        IS_UNIDIRECTIONAL_RUNNING = False
            
        if case_id in RETRY_IN_PROGRESS:
            logger.info(f"[RETRY-API] Skipping retry for case {case_id} — already in progress.")
            return
        RETRY_IN_PROGRESS.add(case_id)

    logger.info(" ")
    logger.info("***  CASE CREATION PROCESS STARTED  ***")
    logger.info("*****************************************************")
    
    executor = ThreadPoolExecutor(max_workers=1)
    
    try:
        logger.info(f"[RETRY-API] Starting retry for case_id={case_id}, machine_id={machine_id}, sample_type={sample_type}")
        
        # Map sample_type to number
        sample_type_number = {"S1": 1, "S2": 2, "S3": 3, "S4": 4}.get(sample_type, 1)
            
        case_entry = {
            "case_id": case_id,
            "test_id": test_id,
            "sample_type": sample_type,
            "sample_type_number": sample_type_number,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Wrap create_case_in_machine in a future with timeout
        future = executor.submit(
            create_case_in_machine,
            connection,
            case_id,
            test_id,
            sample_type,
            sample_type_number
        )
        
        try:
            
            # Wait max 10 minutes
            response = future.result(timeout=600) 
             
            # IF U WANT TO LOG PROCESSING TIME...
            # logger.info(f"[RETRY-API] Waiting for case creation (max 10 minutes) — case_id={case_id}")
            # response = future.result(timeout=600)  # ⏱ 10 minutes
            # logger.info(f"[RETRY-API] Response received for case_id={case_id}: {response}")

        except TimeoutError :
            response = {"status": "error", "message": "Timeout: Case creation took more than 10 minutes"}
            logger.error(f"[TIMEOUT] Case {case_id} creation timed out after 10 minutes.")
            
        except Exception as e :
            response = {"status": "error", "message": f"{e}"}
            logger.error(f"{response['message']}")
            
        if response["status"] == "success":
            case_entry["status"] = "Success"
            add_case_to_json(case_entry, machine_id, 'complete_case_creation.json')
            logger.info(f"[RETRY-API] ✅ Case {case_id} created successfully and saved in complete_case_creation.json")
            
            if is_case_in_pending_cases(case_id):
                delete_pending_case(case_id)

        else:
            case_entry["status"] = "Error"
            add_case_to_json(case_entry, machine_id, 'error_case_creation.json')
            insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, machine_id)
            logger.warning(f"[RETRY-API] ❌ Case {case_id} failed — status: {response.get('status')}, message: {response.get('message')}")

        return response
       
    except Exception as e:
        error_message = f"[EXCEPTION] Error in retry_api_call for case_id={case_id}"
        send_mail_or_logging(error_message, e)
        case_entry["status"] = "Exception Error"
        add_case_to_json(case_entry, machine_id, 'error_case_creation.json')
        insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, machine_id)
        
        msg = f'Exception {e}'
        response['status'] = msg
        return response
        
    finally:
        with FLAG_LOCK:
            RETRY_IN_PROGRESS.discard(case_id)
            IS_BIDIRECTIONAL_RUNNING = False
        executor.shutdown(wait=False)
        logger.info(f"[RETRY-API] Finished processing case_id={case_id}")



# ✅ 8. File Writing & Data Reception - UNIDIRECTION
def write_data_to_file(file, byte_array):
    """Write data to a file safely."""
    try:
        file.write(''.join(byte_array))
    except Exception as e:
        error_message = 'Error in write data in txt file'
        send_mail_or_logging(error_message, e)
        raise

def continuous_receiving_data(connection):
    """Continuously receive data from the machine."""
    global IS_RECIEVING_DATA, IS_UNIDIRECTIONAL_RUNNING, IS_BIDIRECTIONAL_RUNNING, IS_PENDING_RETRY_RUNNING
    byte_array = []
    file = None
    last_byte_time = time.time() 
    try:
        logger.info("[UNI] Started unidirectional reading...")
        
        # Use while True: with a flag check inside the loop guarded by the lock. This way, if IS_RECIEVING_DATA becomes False from another thread, the loop cleanly exits next iteration.
        while True:
            with FLAG_LOCK:
                if not IS_RECIEVING_DATA:
                    break  # exit if flag turned off
            
            with LOCK:
                if connection:
                    try:
                        byte = connection.read()
                        if byte:
                            last_byte_time = time.time()
                        else:
                            if time.time() - last_byte_time > 5:  # 5s timeout
                                logger.info("[UNI] No data for 5s in continuous_receiving_data → stopping.")
                                break
                            continue
                        
                        logger.info(f"[UNI] Read byte (continuous_receiving_data): {byte}")
                        byte_array.append(chr(ord(byte)))

                        if byte == b'\x05':  # Start of new data
                            byte_array = [chr(ord(byte))]
                            connection.write(b'\x06')  # Send ACK
                            cur_file = connection.get_filename()
                            try:
                                if file:
                                    file.close()
                                file = open(cur_file, 'w')
                                write_data_to_file(file, byte_array)
                            except IOError as e:
                                send_mail_or_logging('[UNI] Error in creating new txt file', e)

                        elif byte == b'\x0a':  # Data block finished
                            try:
                                connection.write(b'\x06')
                                write_data_to_file(file, byte_array)
                                byte_array = []
                            except Exception as e:
                                send_mail_or_logging('[UNI] Error writing data to txt file', e)

                        elif byte == b'\x04':  # End of data transmission
                            try:
                                if file:
                                    write_data_to_file(file, byte_array)
                                    logger.info(f"[UNI] Data written to file: {file}")
                                    file.close()
                                byte_array = []
                                
                                # ✅ Stop UNI mode immediately after EOT
                                with FLAG_LOCK:
                                    IS_UNIDIRECTIONAL_RUNNING = False
                                    IS_RECIEVING_DATA = False
                            except Exception as e:
                                send_mail_or_logging('[UNI] Error closing file', e)

                    except Exception as e:
                        send_mail_or_logging('[UNI] Error in reading data', e)

        logger.info("[UNI] Stopped unidirectional reading due to IS_RECIEVING_DATA=False.")

    except Exception as e:
        send_mail_or_logging('[UNI] Error in continuous_receiving_data()', e)
    finally:
        with FLAG_LOCK:
            IS_UNIDIRECTIONAL_RUNNING = False
            IS_RECIEVING_DATA = False
        


# ✅ 9. FastAPI App Setup 
app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request schema for create_case
class CreateCaseRequest(BaseModel):
    case_id: str
    test_id: str
    sample_type: str
    machine_id: str

# API routes
@app.post(f"/create_case/{HOST_COM_PORT}")
async def api_create_case(request: CreateCaseRequest, background_tasks: BackgroundTasks):
    """API endpoint to create a case."""
    global IS_UNIDIRECTIONAL_RUNNING, IS_BIDIRECTIONAL_RUNNING, IS_PENDING_RETRY_RUNNING 
    logger.info(f"[BI-API] Received request: {request.model_dump()}")
    
    try:
        
        sample_type_number = {"S1": 1, "S2": 2, "S3": 3, "S4": 4}.get(request.sample_type, 1)
        
        with FLAG_LOCK:
            uni_running = IS_UNIDIRECTIONAL_RUNNING
            pending_retry_running = IS_PENDING_RETRY_RUNNING
            
        if uni_running or pending_retry_running:
            # ✅ Scenario 1: UNI or pending retry is running → store in pending
            reason = []
            if uni_running:
                reason.append("UNI is running")
            if pending_retry_running:
                reason.append("Pending retry is running")
            reason_str = " & ".join(reason)
            
            logger.warning(f"[BI-API] {reason_str} → storing case in pending DB")
            
            insert_pending_case_to_db(
                case_id=request.case_id,
                test_id=request.test_id,
                sample_type=request.sample_type,
                sample_type_number=sample_type_number,
                machine_id=request.machine_id
            )
            logger.info(f"[BI-API] Stored case in pending case because {reason_str}: {request.case_id}")
            return {"status":200, "statusState":"pending", "message":"Stored in pending"}

        else:
            # ✅ Scenario 2: BI runs and blocks UNI/RETRY
            def run_bi_task():
                global IS_BIDIRECTIONAL_RUNNING
                with FLAG_LOCK:
                    IS_BIDIRECTIONAL_RUNNING = True
                logger.info(f"[BI-API] Starting BI task for case: {request.case_id}")
                    
                try:
                    response = retry_api_call(
                        connection_c_311,
                        request.case_id,
                        request.test_id,
                        request.sample_type,
                        request.machine_id
                    )
                    if response is not None and 'message' in response:
                        logger.info(f"[BI-API] Successfully create_case_to_machine func called for {request.case_id} && Response: {response['message']}")
                    else:
                        logger.error(f"[BI-API] Unexpected or None response for case {request.case_id}: {response}")
                except Exception as e:
                    logger.error(f"[BI-API] Error in BI task for case {request.case_id}: {e}")
                    send_mail_or_logging("Error in BI task", e)
                finally:
                    with FLAG_LOCK:
                        IS_BIDIRECTIONAL_RUNNING = False
                    logger.info(f"[BI-API] BI task ended for case: {request.case_id}")
                    logger.info(f"=" * 80)
                    

            background_tasks.add_task(run_bi_task)
            return {"status": 200, "statusState": "success", "message": "Sent to machine"}
  
    except Exception as e:
        error_message = 'Error in main api calling'
        send_mail_or_logging(error_message, e)
        raise HTTPException(
            status_code=400,
            detail={
                "status": 400,
                "statusState": "error",
                "message": f"Case Creation Error: {e}"
            }
        )
     


# ✅ 10. Background Threads Setup
def schedule_pending_retry(connection, interval_sec=600):
    """
    Every `interval_sec` seconds, check DB for pending cases.
    Runs exactly one pass of check_and_retry_pending_cases if BI/UNI not running.
    """
    while True:
        try:
            with FLAG_LOCK:
                if IS_BIDIRECTIONAL_RUNNING or IS_UNIDIRECTIONAL_RUNNING:
                    logger.info("[PENDING]" + "=" * 100)
                    logger.info("[PENDING] ⏸ Retry skipped — BI/UNI active")
                    logger.info("[PENDING]" + "=" * 100 + "\n")
                    time.sleep(interval_sec)
                    continue

            # Count pending rows safely
            count = 0
            try:
                conn = db_pool.get_connection()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM pending_cases WHERE machine_id = %s",
                            (MACHINE_ID,)
                        )
                        count = cur.fetchone()[0]
                    conn.close()
            except Exception as e:
                logger.warning("[PENDING] DB count check failed", exc_info=True)

            if count > 0:
                result = check_and_retry_pending_cases(connection)
                logger.info(f"[PENDING] ✅ Retry cycle finished → Result: {result}")
            else:
                logger.debug("[PENDING] No pending cases — Skipping retry.")

        except Exception as e:
            send_mail_or_logging("[PENDING] Error in scheduled pending retry", e)

        finally:
            logger.info(f"[PENDING] 💤 Sleeping until next scheduled retry... \n{'=' * 100}\n")
            time.sleep(interval_sec)
            
def monitor_flags_and_manage_threads(connection):
    """
    Continuously monitor:
      • If Uni is on (and Bi off): read from machine.
      • Reset retry guard whenever Uni or Bi comes back on.
    """
    global IS_RECIEVING_DATA, IS_UNIDIRECTIONAL_RUNNING
    last_byte_time = time.time()  # ✅ Initialize once
    logger.info("Started continuous monitoring for machine data...")

    while True:
        try:
            with FLAG_LOCK:
                uni_running = IS_UNIDIRECTIONAL_RUNNING
                bi_running = IS_BIDIRECTIONAL_RUNNING

            # ——— Uni-directional allowed ———
            if not bi_running:
                byte = connection.read()
                logger.info(f"[UNI] Read byte (monitor_flags_and_manage_threads): {byte}, IS_UNI_RUNNING: {uni_running}")
    
                # ✅ Start UNI only when ENQ (0x05) received
                if not uni_running and byte == b'\x05':
                    logger.info(f"[UNI] ENQ {byte} received → starting unidirectional mode")
                    with FLAG_LOCK:
                        last_byte_time = time.time()  # ✅ reset on start
                        logger.info(f"{last_byte_time}")
                        IS_UNIDIRECTIONAL_RUNNING = True
                        IS_RECIEVING_DATA = True
                    continuous_receiving_data(connection)

                
                elif uni_running:
                    # Monitor for timeout — stop UNI if no data for 2s
                    if byte:
                        last_byte_time = time.time()
                    if time.time() - last_byte_time > 2:
                        logger.info("[UNI] Timeout reached → stopping unidirectional mode")
                        with FLAG_LOCK:
                            IS_UNIDIRECTIONAL_RUNNING = False
                            IS_RECIEVING_DATA = False
                        last_byte_time = time.time()  # ✅ reset after stopping

            # ——— Bi-directional or Idle ———
            else:
                # BI is running → ensure UNI stopped
                with FLAG_LOCK:
                    IS_UNIDIRECTIONAL_RUNNING = False
                    IS_RECIEVING_DATA = False  

                if bi_running:
                    logger.debug("Monitoring paused – API in progress")
                time.sleep(0.1)  # prevent tight loop

        except Exception as e:
            send_mail_or_logging("Critical exception in monitoring loop", e)
        time.sleep(0.1)

def start_monitoring():
    """Start the monitoring thread"""
    try:
        monitoring_thread = threading.Thread( target=monitor_flags_and_manage_threads, args=(connection_c_311,),  daemon=True, name="MonitoringThread")
        monitoring_thread.start()
        logger.info("Monitoring thread started")
        
        pending_retry_thread = threading.Thread(target=schedule_pending_retry, args=(connection_c_311, 60), daemon=True, name="PendingRetryThread" )
        pending_retry_thread.start()
        logger.info("pending-retry thread started")

    except Exception as e:
        error_message = 'Failed to establish machine connection'
        send_mail_or_logging(error_message, e)
        
        
 
# ✅ 11. Main Execution - # Main         
if __name__ == "__main__":
    try:
        logger.info(f"Starting FastAPI server on {HOST_ADDRESS}:{HOST_PORT}")
        
        # Start monitoring before starting the server
        start_monitoring()
        
        JSON_FILE_LIST = [
            os.path.join(CASE_FILE_PATH, 'complete_case_creation.json'),
            os.path.join(CASE_FILE_PATH, 'error_case_creation.json'),
        ]
        threading.Thread(target=remove_old_case_entries_from_json_files, args=(JSON_FILE_LIST, 10, 86400), daemon=True).start()
        
        threading.Thread(target=remove_old_logs_from_log_file, args=(LOG_FILE_LIST,), daemon=True).start()

        # Start the FastAPI server
        uvicorn.run(app, host=HOST_ADDRESS, port=HOST_PORT)
        
    except Exception as e:
        error_message = 'Exception in main function'
        send_mail_or_logging(error_message, e)


