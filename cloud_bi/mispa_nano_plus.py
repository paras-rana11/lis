import os
import re
import time
import json
import socket
import threading
import uvicorn # type: ignore
import smtplib 
import logging
import asyncio
from typing import List, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel # type: ignore
from email.message import EmailMessage
from fastapi.middleware.cors import CORSMiddleware # type: ignore
from fastapi import FastAPI, HTTPException, BackgroundTasks # type: ignore

import socket




REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'
CASE_FILE_PATH = 'C:\\ASTM\\root\\case_file\\'
LOG_FILE_PATH = 'C:\\ASTM\\root\\log_file\\'



class MachineConnectionTcp:
    def __init__(self, server_ip=None, server_port=None, machine_name=None):
        if server_ip and server_port and machine_name:
            self.connection = None
            self.server_socket = None  
            self.server_ip = server_ip
            self.server_port = server_port
            self.machine_name = machine_name
            self.is_connected = False 

    def get_filename(self):
        dt = datetime.now()
        return REPORT_FILE_PATH + f"{self.machine_name}_{dt.strftime('%Y-%m-%d-%H-%M-%S-%f')}.txt"

    def get_connection(self):
        if self.is_connected and self.connection:
            print("Using existing connection")
            return self.connection
            
        try:            
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(1)
            self.server_socket.settimeout(30.0) 
            
            # Accept connection
            self.connection, addr = self.server_socket.accept()
            self.connection.settimeout(1.0)
            self.is_connected = True
            print(f"Server Connected To {self.server_ip}:{self.server_port} : {self.connection}")
            
            return self.connection
                
        except Exception as e:
            print(f"Failed to Connect {self.server_ip} : {self.server_port} ::- {e}")
            self.cleanup()
            return None, None

    def read(self):
        if not self.connection:
            raise Exception("Connection is not initialized. Call get_connection() first.")
        try:
            data = self.connection.recv(1)
            return data
        except socket.timeout:
            return b''  # Return empty bytes on timeout
        except Exception as e:
            print(f"Error reading data: {e}")
            return b''
    
    def recv(self, buffer_size=1):
        """Alternative method name for consistency"""
        return self.read() if buffer_size == 1 else self.connection.recv(buffer_size)
    
    def process_data(self, data):
        if data:
            return data.decode('utf-8')
        return ""
    
    def write(self, byte_data):
        if not self.connection:
            raise Exception("Connection is not initialized. Call get_connection() first.")
        try:
            self.connection.sendall(byte_data)
            print(f"Data sent: {byte_data}")
        except Exception as e:
            print(f"Error sending data: {e}")

    def sendall(self, byte_data):
        """Alternative method name for consistency"""
        self.write(byte_data)

    def cleanup(self):
        """Properly close both client and server sockets"""
        if self.connection:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
            except:
                pass
            self.connection = None
            
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
            self.server_socket = None
            
        self.is_connected = False  # Reset connection status

    def close_connection(self):
        self.cleanup()

    def __del__(self):
        """Ensure cleanup on object destruction"""
        self.cleanup()

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


app = FastAPI()

# Initialize connection and other objects
generate_checksum = GenerateChecksum()

STOP_THREAD = False
CASE_CREATION_FLAG = False
MONITORING_ACTIVE = True  # Flag to control monitoring loop
CONNECTION_MISPA_NANO = True

CONNECTION_TYPE = "TCP/IP"
HOST_ADDRESS = "192.168.1.160"
HOST_PORT = 6040
HOST_CONNECTION_PORT = 5040
REPORT_FILE_PREFIX = 'MISPA_NANO'
MACHINE_ID = 'MISPA_NANO'

# log file path
CASE_FILE = 'C:\\ASTM\\root\\case_file\\complete_case_MISPA_NANO.json'

LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_MISPA_NANO_info.log'
ERROR_LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_MISPA_NANO_error.log'
LOG_FILE_LIST = [LOG_FILE, ERROR_LOG_FILE]

# Email configuration
SUBJECT = 'Email From Sanghvi Hospital'
TO_EMAIL = 'lishealthray@gmail.com'
FROM_EMAIL = 'lishealthray@gmail.com'
PASSWORD = 'rxbr zlzy tpin pgur'
 
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
    patient_name: str
    gender: str

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
        logger.setLevel(logging.DEBUG)  
        logger.handlers.clear()  

        # Info Handler
        info_handler = logging.FileHandler(log_file)
        info_handler.setLevel(logging.INFO)
        info_format = logging.Formatter('%(asctime)s - INFO - %(message)s')
        info_handler.setFormatter(info_format)

        # Error Handler
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_format = logging.Formatter('%(asctime)s - ERROR - %(message)s')
        error_handler.setFormatter(error_format)

        logger.addHandler(info_handler)
        logger.addHandler(error_handler)

        return logger
    except Exception as e:
        error_message = 'Exception in initialize logger'
        send_mail_or_logging(error_message, e)
        return logger

logger = setup_loggers(LOG_FILE, ERROR_LOG_FILE)
logger.info('Log file initialized.')
logger.error('This is an error log example.')

def send_mail_or_logging(error_message, error):
    logger.error(f'{error_message} : {error}')

    body = f"""
    Dear Team,

    This is an automated alert from the Laboratory Information System.

        --> {error_message} :: Below are the details of the incident:

        - Filename: MISPA_NANO_by.py
        - Connetion Type: {CONNECTION_TYPE}
        - Machine Name: {HOST_ADDRESS}
        - Port: {HOST_PORT}
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
        error_message = 'Exception in send ASTM Message'
        send_mail_or_logging(error_message, e)

def write_data_to_file(file, byte_array):
    """Write data to a file safely."""
    try:
        file.write(''.join(byte_array))
    except Exception as e:
        error_message = 'Exception in rewrite data to file'
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

            # time.sleep(sleep_interval)
            time.sleep(3)
            

    except Exception as e:
        send_mail_or_logging("Fatal error in remove_old_case_entries_from_files", e)



def continuous_receiving_data(connection, initial_byte):
    """Continuously receive data from the machine."""
    global STOP_THREAD
    byte_array = []
    file = None

    def handle_data_block(byte):
        nonlocal byte_array
        byte_array.append(chr(ord(byte)))

    def handle_start_of_data(byte):
        nonlocal file, byte_array
        byte_array = [chr(ord(byte))]
        connection.write(b'\x06')  # Send ACK
        cur_file = connection.get_filename()
        if file:
            file.close()
        file = open(cur_file, 'w')
        write_data_to_file(file, byte_array)

    def handle_end_of_block():
        nonlocal byte_array
        connection.write(b'\x06')
        write_data_to_file(file, byte_array)
        byte_array = []

    def handle_end_of_transmission():
        nonlocal file, byte_array
        global STOP_THREAD
        if file:
            write_data_to_file(file, byte_array)
            logger.info(f'Data written to file: {file.name}')
            file.close()
        byte_array = []
        STOP_THREAD = False

    try:
        logger.info("Started unidirectional reading...")

        # Process the initial byte if provided
        if initial_byte == b'\x05':
            try:
                handle_start_of_data(initial_byte)
            except Exception as e:
                send_mail_or_logging('Exception in open file', e)

        while STOP_THREAD and connection:
            try:
                byte = connection.read()
                if not byte:
                    continue

                if byte == b'\x05':
                    try:
                        handle_start_of_data(byte)
                    except Exception as e:
                        send_mail_or_logging('Exception in open file', e)

                elif byte == b'\x0a':
                    try:
                        handle_end_of_block()
                    except Exception as e:
                        send_mail_or_logging('Exception in write file', e)

                elif byte == b'\x04':
                    try:
                        handle_end_of_transmission()
                    except Exception as e:
                        send_mail_or_logging('Exception in close file', e)

                else:
                    handle_data_block(byte)

            except Exception as e:
                send_mail_or_logging('Exception in data receive', e)

        logger.info("Stopped unidirectional reading.")
        
    except Exception as e:
        send_mail_or_logging('Exception in continuous receiving data', e)


# Method for create case in machine
def create_case_in_machine(connection, case_no, test_id, sample_type, sample_type_number, patient_name, gender):
    """Send commands to the machine to create a case."""
    global byte
    try:
        dt = datetime.now()
        logger.info(" ")
        logger.info("***  CASE CREATION PROCESS STARTED  ***")
        logger.info("*****************************************************")
        connection.write(generate_checksum.ENQ.encode())
        byte = connection.read()
        if byte == generate_checksum.ACK.encode():
            # Step 1: Send header message
            header_message = rf"1H|\^&|||Mindry^^|||||||SA|1394-97|{dt.strftime('%Y%m%d%H%M%S%f')}"
            send_message(connection, header_message)
            if connection.read() != generate_checksum.ACK.encode():
                return {"status": "error", "message": "Header Message Not Acknowledged"}
            logger.info(f"HEADER MESSAGE :-       {header_message}")

            # Step 2: Send patient message
            patient_id = re.sub(r'[A-Za-z]', '', case_no)
            if '-' in patient_id:
                patient_id = patient_id.split('-')[0]
            patient_message = f"2P|1||{patient_id}||{patient_name}||19600315|{gender}|||A|||icteru||||||01|||||A1|002||||||||"
            send_message(connection, patient_message)
            if connection.read() != generate_checksum.ACK.encode():
                return {"status": "error", "message": "Patient Message Not Acknowledged"}
            logger.info(f"PATIENT MESSAGE :-      {patient_message}")

            # Step 3: Send order message
            order_message = f"3O|1||{case_no}|{test_id}|R|{dt.strftime('%Y%m%d%H%M%S%f')}|{dt.strftime('%Y%m%d%H%M%S%f')}||||||||{sample_type}|||{sample_type_number}|||||||O|||||"
            send_message(connection, order_message)
            if connection.read() != generate_checksum.ACK.encode():
                return {"status": "error", "message": "Order Message Not Acknowledged"}
            logger.info(f"ORDER MESSAGE :-        {order_message}")

            # Step 4: Send termination message
            termination_message = f"4L|1|N"
            send_message(connection, termination_message)
            if connection.read() == generate_checksum.ACK.encode():
                connection.write(generate_checksum.EOT.encode())
                logger.info(f"TERMINATION MESSAGE :-  {termination_message}")
                logger.info("*****************************************************\n")
                return {"status": "success", "message": "Case created successfully", "case_no":case_no}
            else:
                return {"status": "error", "message": "Termination Message Not Acknowledged", "case_no":case_no}
        else:
            return {"status": "error", "message": "ENQ Message Not Acknowledged", "case_no":case_no}
    except Exception as e:
        error_message = 'Exception during case creation process'
        send_mail_or_logging(error_message, e)
        return {"status": "error", "message": str(e)}
        
# Method for calling create case function
# def retry_api_call(connection, case_id, test_id, sample_type, machine_id, patient_name, gender):
#     """Retry API call to ensure the case is created."""
#     global CASE_CREATION_FLAG, STOP_THREAD, MONITORING_ACTIVE
    
#     try:
#         STOP_THREAD = False
#         counter = 0
#         if sample_type == "serum":
#             sample_type_number = 1
#         elif sample_type == "urin":
#             sample_type_number = 2
#         elif sample_type == "csf":
#             sample_type_number = 3
#         elif sample_type == "other":
#             sample_type_number = 4
#         else:
#             sample_type_number = 1
            

            
#         while True:
#             try:
#                 if not STOP_THREAD:
#                     response = create_case_in_machine(connection, case_id, test_id, sample_type, sample_type_number, patient_name, gender)
#                     if response["status"] == "success":
#                         case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "machine_id":machine_id, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "status":"Complate"}
#                         add_case_to_json(case_entry, CASE_FILE)
#                         logger.info(f"COUNTER :- {counter}")
#                         CASE_CREATION_FLAG = False
                        
#                         MONITORING_ACTIVE = True
#                         logger.info("Case creation completed, resuming monitoring...")
#                         return response
                    
#             except Exception as e:
#                 error_message = 'Error in API call'
#                 send_mail_or_logging(error_message, e)
#                 case_entry = {"case_id": case_id, "test_id": test_id, "sample_type": sample_type, "sample_type_number": sample_type_number, "machine_id":machine_id, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "status":"Uncomplate"}
#                 add_case_to_json(case_entry, CASE_FILE)

#             counter+=1
#             time.sleep(3)
#     except Exception as e:
#         error_message = 'Exception in retry api call'
#         send_mail_or_logging(error_message, e)
#     finally:
#         MONITORING_ACTIVE = True


def bypass_retry_api_call(connection, case_id, test_id, sample_type, machine_id, patient_name, gender):
    """Bypassed function for testing case creation without machine."""
    global CASE_CREATION_FLAG, STOP_THREAD, MONITORING_ACTIVE

    try:
        STOP_THREAD = False
        counter = 0
        sample_type_number = {
            "serum": 1, "urin": 2, "csf": 3, "other": 4
        }.get(sample_type, 1)

        # Fake wait to simulate process
        time.sleep(2)

        # Simulate success
        case_entry = {
            "case_id": case_id,
            "test_id": test_id,
            "sample_type": sample_type,
            "sample_type_number": sample_type_number,
            "machine_id": machine_id,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "status": "Complate"
        }
        add_case_to_json(case_entry, MACHINE_ID, CASE_FILE)
        logger.info(f"Case '{case_id}' created successfully in mock mode.")

    except Exception as e:
        error_message = 'Exception in bypassed retry api call'
        send_mail_or_logging(error_message, e)

    finally:
        CASE_CREATION_FLAG = False
        MONITORING_ACTIVE = True



def checking_api_calling_and_data_reading(connection):
    """Continuously monitor for data from machine - starts on startup"""
    global STOP_THREAD, CASE_CREATION_FLAG, MONITORING_ACTIVE
    
    logger.info("Started continuous monitoring for machine data...")
    
    while True: 
        try:
            if MONITORING_ACTIVE and not CASE_CREATION_FLAG:
                try:
                    if hasattr(connection, 'settimeout'):
                        connection.settimeout(1.0)
                    
                    byte = connection.read()
                    
                    if byte and byte != b'':
                        STOP_THREAD = True
                        # continuous_receiving_data(connection, byte)
                        
                    elif byte == generate_checksum.ACK.encode():
                        logger.info("Received ACK from machine")
                        
                except socket.timeout:
                    pass
                except Exception as e:
                    if "timed out" not in str(e).lower():
                        send_mail_or_logging('Exception while monitoring data', e)
                        
            else:
                if CASE_CREATION_FLAG:
                    logger.info("Monitoring paused - API call in progress")
                time.sleep(0.5)  
                
        except Exception as e:
            send_mail_or_logging('Critical exception in monitoring loop', e)
            time.sleep(5) 
            
        time.sleep(0.1)  

# API routes
@app.post(f"/create_case/P{HOST_CONNECTION_PORT}")
async def api_create_case(request: CreateCaseRequest, background_tasks: BackgroundTasks):
    """API endpoint to create a case."""
    global CASE_CREATION_FLAG, MONITORING_ACTIVE
    
    try:
        logger.info(f"API called for case creation: {request.case_id}")
        
        
        # Stop monitoring temporarily while processing API
        CASE_CREATION_FLAG = True
        MONITORING_ACTIVE = False
        logger.info("Monitoring paused for API processing")
        
        # Wait a moment for monitoring to pause
        await asyncio.sleep(0.5)
        
        # Add the case creation task to background
        background_tasks.add_task(
            # retry_api_call,
            # CONNECTION_MISPA_NANO,
            bypass_retry_api_call,
            None,
            request.case_id,
            request.test_id,
            request.sample_type,
            request.machine_id,
            request.patient_name,
            request.gender
        )
        
        # NOTE: 2 line for testing
        case_entry = {"case_id": request.case_id, "test_id": request.test_id, "sample_type": request.sample_type, "sample_type_number": 'request.sample_type_number', "machine_id":request.machine_id, "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "status":"Complate"}
        add_case_to_json(case_entry, MACHINE_ID, CASE_FILE)
        
        return {"status": 200, "statusState": "success", "message": "Case Creation Started"}
        
    except Exception as e:
        error_message = 'Exception in api call'
        send_mail_or_logging(error_message, e)
        
        # Resume monitoring even if API fails
        CASE_CREATION_FLAG = False
        MONITORING_ACTIVE = True
        
        raise HTTPException(
            status_code=400,
            detail={
                "status": 400,
                "statusState": "error",
                "message": f"Case Creation Error: {e}"
            }
        )

# Try to establish connection
try:
    pass
    # CONNECTION_MISPA_NANO = MachineConnectionTcp(
    #     server_ip=HOST_ADDRESS,
    #     server_port=HOST_CONNECTION_PORT, 
    #     machine_name=REPORT_FILE_PREFIX
    # )
    
    # port_MISPA_NANO = CONNECTION_MISPA_NANO.get_connection()
    # logger.info(f"Connection established on port: {port_MISPA_NANO}")
except Exception as e:
    error_message = 'Failed to establish machine connection'
    send_mail_or_logging(error_message, e)

# Start monitoring thread on startup
def start_monitoring():
    """Start the monitoring thread"""
    try:
        monitoring_thread = threading.Thread(
            target=checking_api_calling_and_data_reading, 
            args=(CONNECTION_MISPA_NANO,), 
            daemon=True,
            name="MonitoringThread"
        )
        monitoring_thread.start()
        logger.info("Monitoring thread started")

    except Exception as e:
        error_message = 'Failed to establish machine connection'
        send_mail_or_logging(error_message, e)

def check_connection():
    try:
        while True:
            if not CONNECTION_MISPA_NANO.is_connected:
                port_MISPA_NANO = CONNECTION_MISPA_NANO.get_connection()
            time.sleep(3600)
            
    except Exception as e:
        error_message = 'Failed to establish machine connection'
        send_mail_or_logging(error_message, e)

# Main
if __name__ == "__main__":
    try:
        logger.info(f"Starting FastAPI server on {HOST_ADDRESS}:{HOST_PORT}")
        
        # Start monitoring before starting the server
        # start_monitoring()
        
        JSON_FILE_LIST = [
            os.path.join(CASE_FILE_PATH, 'complete_case_MISPA_NANO.json'),
            os.path.join(CASE_FILE_PATH, 'error_case__MISPA_NANO.json'),
        ]
        threading.Thread(target=remove_old_case_entries_from_files, args=(JSON_FILE_LIST, 10, 86400), daemon=True ).start()
        threading.Thread(target=remove_old_logs_from_log_file, args=(LOG_FILE_LIST,), daemon=True).start()
        # threading.Thread(target=check_connection, daemon=True).start()

        # Start the FastAPI server
        uvicorn.run(app, host=HOST_ADDRESS, port=HOST_PORT)
        
    except Exception as e:
        error_message = 'Exception in main function'
        send_mail_or_logging(error_message, e)