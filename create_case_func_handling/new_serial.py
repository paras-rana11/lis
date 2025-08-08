# handling create_case-in_machine fails then add that case entry to db and retry after 10 mins


from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from threading import RLock, Thread
from typing import List, Dict, Any
import uvicorn
import time
import json
import os
import logging
import threading
import smtplib
from email.message import EmailMessage

import sys
import serial  # type: ignore
import mysql.connector
from random import randint




REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'
CASE_FILE_PATH = 'C:\\ASTM\\root\\case_file\\'
LOG_FILE_PATH = 'C:\\ASTM\\root\\log_file\\'


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

class ConnectionDb:
    def __init__(self , host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def connect_db(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if conn:
                print("*******************************************")
                print("***********  DATABASE CONNECTED ***********")
                print("*******************************************")
                return conn
            else:
                return "Connectiona Not Established"
        except Exception as e:
            print(f"Connection Error : {e}")
            
def get_connection():
    try:
        db = ConnectionDb(
            host="localhost",
            user="root",
            password="root1",
            database="lis"
        )
        return db.connect_db()
    except Exception as e:
        logger.error(f"Error creating DB connection: {e}")
        return None


# Global objects and variables
generate_checksum = GenerateChecksum()
LOCK = RLock()
STOP_THREAD = False

# Machine Configuration
MACHINE_NAME = 'Cobas C 311 1'
HOST_ADDRESS = '127.0.0.1'
HOST_PORT = 6010
HOST_COM_PORT = 'COM1'
MACHINE_ID = 'C_311_1'
REPORT_FILE_PREFIX = 'c_311_1_'
CONNECTION_TYPE = 'serial'

LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_c_311_1.log'
ERROR_LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_c_311_1_error.log'
PENDING_CASE_LOG_FILE = 'C:\\ASTM\\root\\log_file\\pending_cases_log.log'
LOG_FILE_LIST = [LOG_FILE, ERROR_LOG_FILE, PENDING_CASE_LOG_FILE]

log_check_interval = 86400  # 24 hours (in seconds)

SUBJECT = "Email From SGL Hospital"
TO_EMAIL = "lishlray@gmail.com"
FROM_EMAIL = "lishlray@gmail.com"
PASSWORD = "rxbr zlzy tpin pgur" 





           
# connection_c_311 = MachineConnectionSerial(
#     connection_type=CONNECTION_TYPE,
#     input_tty=HOST_COM_PORT, 
#     machine_name=REPORT_FILE_PREFIX
# )

# Try to establish connection
# port_c_311, path = connection_c_311.get_port()

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

        - Filename: cobas_c_311_1.py
        - Connetion Type: {CONNECTION_TYPE}
        - Machine Name: {MACHINE_NAME}
        - Machine Host Address: {HOST_ADDRESS}
        - Port: {HOST_PORT}
        - COM Port: {HOST_COM_PORT}
        - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        - Error Details: {error}

    Best regards,  
    Laboratory Information System  
    [hlray LAB]
    """

    # is_send_mail = send_email(SUBJECT, body, TO_EMAIL, FROM_EMAIL, PASSWORD)
    # if not is_send_mail:
    #     logger.error(f"Exception during sending mail")

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

                with open(file,'w') as f:
                    f.write('\n'.join(final_line_list))
            
            time.sleep(86400)
    except Exception as e:
        error_message = 'Error in remove old log'
        send_mail_or_logging(error_message, e)


# Helper functions
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


def write_data_to_file(file, byte_array):
    """Write data to a file safely."""
    try:
        file.write(''.join(byte_array))
    except Exception as e:
        error_message = 'Error in write data in txt file'
        send_mail_or_logging(error_message, e)
        raise



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
        send_mail_or_logging("Fatal error in remove_old_case_entries_from_files", e)


def is_case_already_pending(case_no):
    try:
        conn = get_connection()
        if not conn:
            logger.error("[PENDING] DB connection not available in is_case_already_pending")
            return False
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM pending_case WHERE case_no = %s"
        cursor.execute(query, (case_no,))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        send_mail_or_logging("[PENDING] Error in is_case_already_pending", e)
        return False  


def insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, machine_id):
     
    try:
        conn = get_connection()
        if not conn:
            logger.error("[PENDING] DB connection not available in insert_pending_case_to_db")
            return
        cursor = conn.cursor()
        query = """
            INSERT INTO pending_cases (case_id, test_id, sample_type, sample_type_number, machine_id)
            VALUES (%s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                test_id = new.test_id,
                sample_type = new.sample_type,
                sample_type_number = new.sample_type_number,
                machine_id = new.machine_id    
        """
        values = (case_id, test_id, sample_type, sample_type_number, machine_id)
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"[PENDING] Inserted to pending_cases: {case_id}")
    except Exception as e:
        send_mail_or_logging("[PENDING] Error inserting into pending_cases", e)


def check_and_retry_pending_cases(connection):
    try:
        while True:
            try:
                logger.info("[PENDING] \n" + "=" * 100)
                logger.info("[PENDING]  Starting new retry cycle")
                logger.info("[PENDING] " + "=" * 100)
                
                conn = get_connection()
                if conn is None:
                    logger.warning("[PENDING] ⚠ DB connection failed. Will retry in 1 minute.")
                    time.sleep(60)
                    continue
                
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM pending_cases")
                rows = cursor.fetchall()
                logger.info(f"[PENDING] Retry thread running at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                
                if not rows:
                    logger.info("[PENDING]  No pending cases found.")
                else:
                    logger.info(f"[PENDING]  Found {len(rows)} pending cases.")

                for row in rows:
                    try:
                        logger.info("[PENDING] " +  "="*80)
                        logger.info(f"[PENDING]  Retrying case: {row['case_id']}")
                        
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

                            del_query = "DELETE FROM pending_cases WHERE id = %s"
                            cursor.execute(del_query, (row["id"],))
                            conn.commit()
                            logger.info(f"[PENDING]  Case {row['case_id']} successfully processed and removed from DB. ✔️")
                        else:
                            add_case_to_json(case_entry, row["machine_id"], 'error_case_creation.json')
                            logger.warning(f"[PENDING]  Retry failed for Case {row['case_id']}: {response.get('message')}. ❌")

                    except Exception as e:
                        send_mail_or_logging(f"[PENDING] Error processing case {row['case_id']}.⚠️", e)
                        
                cursor.close()
                conn.close()
            
            except Exception as e:
                logger.exception(f"[PENDING] Exception occurred during retry cycle: {e}. ⚠️")
                
            logger.info("[PENDING] " +  "="*80)
            logger.info("[PENDING] Waiting 1 minute before next retry cycle...\n")
            time.sleep(60)
              
    except Exception as e:
        send_mail_or_logging("[PENDING] Error in retry pending case thread", e)
    


def create_case_in_machine(connection, case_no, test_id, sample_type, sample_type_number):
    """Send commands to the machine to create a case."""
    with LOCK:
        try:
            """
            logger.info(" ")
            logger.info("***  CASE CREATION PROCESS STARTED  ***")
            logger.info("*****************************************************")
        
            connection.write(generate_checksum.ENQ.encode())
            byte = connection.read()

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
            """
            logger.info(f"Creating case in machine: {case_no}")
            try:
                num = randint(0, 1)    # generate 0 or 1
                print("\n\nnum:", num)
                if num:  
                    print("success:", num, "=True")
                    return {"status": "success", "message": "Case created"}
                else:
                    print("Fails:", num, "=False")
                    return {"status": "error", "message": "Machine did not respond "}
            except Exception as e:
                return {"status": "error", "message": str(e)}
            
        except Exception as e:
            error_message = '[PENDING] Error in create case in machine'
            send_mail_or_logging(error_message, e)
            return {"status": "error", "message": str(e)}
            
  




def retry_api_call(connection, case_id, test_id, sample_type, machine_id):
    
    """Retry API call to ensure the case is created."""
    try:
        counter = 0
        if sample_type == "S1":
            sample_type_number = 1
        elif sample_type == "S2":
            sample_type_number = 2
        elif sample_type == "S3":
            sample_type_number = 3
        elif sample_type == "S4":
            sample_type_number = 4
        else:
            sample_type_number = 1
        while True:
            try:
                response = create_case_in_machine(connection, case_id, test_id, sample_type, sample_type_number)
                if response["status"] == "success":
                    case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    add_case_to_json(case_entry, machine_id, 'complete_case_creation.json')
                    logger.info(f"COUNTER :- {counter}")
                    return response
                else:
                    case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    add_case_to_json(case_entry, machine_id, 'error_case_creation.json')
                    insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, MACHINE_ID)
                    logger.info(f"COUNTER :- {counter}")
                    return response

            except Exception as e:
                print(f"Error in API call: {e}")
                case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "status":"Error"}
                add_case_to_json(case_entry, machine_id, 'error_case_creation.json')
            counter+=1
            time.sleep(5)
    except Exception as e:
        error_message = 'Error in retry api call'
        send_mail_or_logging(error_message, e)


def continuous_receiving_data(connection):  
    """Continuously receive data from the machine."""
    global STOP_THREAD
    byte_array = []
    file = None  # Initialize file variable here
    try:
        logger.info("Started unidirectional reading...")
        while not STOP_THREAD:
            with LOCK:
                if connection:
                    try:
                        byte = connection.read()
                        if not byte:
                            continue

                        byte_array.append(chr(ord(byte)))

                        if byte == b'\x05':  # Start of new data
                            byte_array = [chr(ord(byte))]
                            connection.write(b'\x06')  # Send ACK
                            cur_file = connection.get_filename()
                            try:
                                if file:  # Close any open file before opening a new one
                                    file.close()
                                file = open(cur_file, 'w')  # Open a new file
                                write_data_to_file(file, byte_array)
                            except IOError as e:
                                error_message = 'Error in creating new txt file'
                                send_mail_or_logging(error_message, e)

                        elif byte == b'\x0a':  # Data block finished
                            try:
                                connection.write(b'\x06')
                                write_data_to_file(file, byte_array)
                                byte_array = []
                            except Exception as e:
                                error_message = 'Error in wite data in txt file'
                                send_mail_or_logging(error_message, e)

                        elif byte == b'\x04':  # End of data transmission
                            try:
                                if file:  
                                    write_data_to_file(file, byte_array)
                                    logger.info(f'Data written to file : {file}')
                                    file.close()
                                byte_array = []
                            except Exception as e:
                                error_message = 'Error in closing file'
                                send_mail_or_logging(error_message, e)

                    except Exception as e:
                        error_message = 'Error in reading data'
                        send_mail_or_logging(error_message, e)

        logger.info("Stopped unidirectional reading.")
    except Exception as e:
        error_message = 'Error in continuous reading data'
        send_mail_or_logging(error_message, e)

def start_receiving_data(connection):
    """Start data receiving thread."""
    try:
        threading.Thread(target=remove_old_logs_from_log_file, args=(LOG_FILE_LIST,), daemon=True).start()
        time.sleep(2)
        receive_thread = Thread(target=continuous_receiving_data, args=(connection,))
        receive_thread.daemon = True
        receive_thread.start()
    except Exception as e:
        error_message = 'Error in receive data function'
        send_mail_or_logging(error_message, e)

def bypass(case_id, test_id, sample_type, machine_id):
    try:
        sample_type_map = {
            "S1": 1,
            "S2": 2,
            "S3": 3,
            "S4": 4
        }
        sample_type_number = sample_type_map.get(sample_type, 1)

        while True:
            try:
                response = create_case_in_machine(case_id, test_id, sample_type, sample_type_number)
                if response["status"] == "success":
                    case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    add_case_to_json(case_entry, machine_id, 'complete_case_creation.json')
                    logger.info(f"COUNTER :- {counter}")
                    return response
                else:
                    insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, MACHINE_ID)
                    return response

            except Exception as e:
                print(f"Error in API call: {e}")
                case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "status":"Error"}
                add_case_to_json(case_entry, machine_id, 'error_case_creation.json')
            counter+=1
            time.sleep(5)
    except Exception as e:
        logger.error(f"Error in bypass function: {e}")
        case_entry = {
            "case_id": case_id,
            "test_id": test_id,
            "sample_type": sample_type,
            "sample_type_number": sample_type_number,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "status": "Error"
        }
        add_case_to_json(case_entry, machine_id, 'error_case_creation.json')
        send_mail_or_logging("Error in bypass case creation", e)


# API routes
@app.post(f"/create_case/{HOST_COM_PORT}")
async def api_create_case(request: CreateCaseRequest, background_tasks: BackgroundTasks):
    """API endpoint to create a case."""
    try:
        background_tasks.add_task(
            # retry_api_call,
            # connection_c_311,
            bypass,
            request.case_id,
            request.test_id,
            request.sample_type,
            request.machine_id
        )
        return {"status": 200, "statusState": "success", "message": "Case Creation Started"}
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

# Main        
if __name__ == "__main__":
    try:
        JSON_FILE_LIST = [
            os.path.join(CASE_FILE_PATH, 'complete_case_creation.json'),
            os.path.join(CASE_FILE_PATH, 'error_case_creation.json'),
        ]
        threading.Thread(target=remove_old_case_entries_from_files, args=(JSON_FILE_LIST, 10, 86400), daemon=True).start()
        # start_receiving_data(connection_c_311)
        # threading.Thread(target=check_and_retry_pending_cases, args=(connection_c_311,), daemon=True).start()
        threading.Thread(target=check_and_retry_pending_cases, args=(None,), daemon=True).start()
        uvicorn.run(app, host=HOST_ADDRESS, port=HOST_PORT)
    except Exception as e:
        send_mail_or_logging('Error in main function', e)
