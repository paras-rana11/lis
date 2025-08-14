# ✅ 1. Imports and Configurations

import os
import time
import json
import threading
import uvicorn # type: ignore
import smtplib 
import logging
from datetime import datetime, timedelta
from pydantic import BaseModel # type: ignore
from email.message import EmailMessage
from fastapi.middleware.cors import CORSMiddleware # type: ignore
from fastapi import FastAPI, HTTPException # type: ignore
from healper import MachineConnectionSerial, ConnectionDb, GenerateChecksum


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

generate_checksum = GenerateChecksum()


# Machine Configuration
MACHINE_NAME = 'Cobas C 311 2'
HOST_ADDRESS = '127.0.0.1'
HOST_PORT = 6020
HOST_COM_PORT = 'COM6'
MACHINE_ID = 'C_311_2'
REPORT_FILE_PREFIX = 'c_311_2_'
CONNECTION_TYPE = 'serial'

# LOCK = threading.Lock()
FLAG_LOCK = threading.Lock()

MONITORING_ACTIVE = True  # Flag to control monitoring loop
IS_RECIEVING_DATA = False
IS_PENDING_RETRY_RUNNING = False

# log file path
COMPLETE_CASE_FILE = 'C:\\ASTM\\root\\case_file\\complete_case_creation_for_c_311_2.json'
ERROR_CASE_FILE = 'C:\\ASTM\\root\\case_file\\error_case_creation_for_c_311_2.json'
CASE_FILE_LIST = [COMPLETE_CASE_FILE, ERROR_CASE_FILE]

LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_c_311_2_info.log'
ERROR_LOG_FILE = 'C:\\ASTM\\root\\log_file\\logging_for_c_311_2_error.log'
LOG_FILE_LIST = [LOG_FILE, ERROR_LOG_FILE]

REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'
CASE_FILE_PATH = 'C:\\ASTM\\root\\case_file\\'
LOG_FILE_PATH = 'C:\\ASTM\\root\\log_file\\'

for folder in [CASE_FILE_PATH, REPORT_FILE_PATH, LOG_FILE_PATH]:
    os.makedirs(folder, exist_ok=True)

# Email configuration
SUBJECT = 'Email From SGL Hospital'
TO_EMAIL = 'lishealthray@gmail.com'
FROM_EMAIL = 'lishealthray@gmail.com'
PASSWORD = 'rxbr zlzy tpin pgur'
 
 
# ✅ 2. Logging Setup
def setup_loggers(log_file, error_log_file):
    '''Set up separate loggers for info and error.'''
    logger = logging.getLogger('app_logger')
    if logger.handlers:  # Don't set up again
        return logger
    try:
        # Ensure directories exist for both log files
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        os.makedirs(os.path.dirname(error_log_file), exist_ok=True)

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

        logger.addHandler(info_handler)
        logger.addHandler(error_handler)

        return logger
    except Exception as e:
        print(f"Error in initialize logger : {e}")
        return logger

logger = setup_loggers(LOG_FILE, ERROR_LOG_FILE)
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
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()  # Secure the connection
            smtp.ehlo()
            smtp.login(from_email, password)
            smtp.send_message(msg)

        logger.info('Email sent successfully!')
        return True
    except Exception as e:
        logger.error(f'Failed to send email: {e}', exc_info=False)     # MAKE tRUE WHEN UNEED TO SEE WHOLE ERROR
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
        logger.info("Thread Start For Remove old Logs\n\n")
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


# ✅ 3. Try to establish connection
try:
    connection_c_311_2 = MachineConnectionSerial(
        connection_type=CONNECTION_TYPE,
        input_tty=HOST_COM_PORT, 
        machine_name=REPORT_FILE_PREFIX
    )
    
    port_c_311_2, path = connection_c_311_2.get_port()
    logger.info(f"Connection established on port: {port_c_311_2}")
except Exception as e:
    error_message = 'Failed to establish machine connection'
    send_mail_or_logging(error_message, e)
           
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
        print(f"Error creating DB connection: {e}")
        return None


# ✅ 4. Utilities
def write_data_to_file(file, byte_array):
    """Write data to a file safely."""
    try:
        file.write(''.join(byte_array))
    except Exception as e:
        error_message = 'Exception in rewrite data to file'
        send_mail_or_logging(error_message, e)
        raise

# Method for load new json file 
def load_json(file_path):
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Open the file and load existing data
        if os.path.exists(file_path):
            with open(file_path, 'r') as json_file:
                try:
                    existing_data = json.load(json_file)
                except json.JSONDecodeError:
                    existing_data = [[]]
        else:
            with open(file_path, 'w') as json_file:
                json.dump([[]], json_file, indent=4)
            existing_data = [[]]
        return existing_data
    
    except Exception as e:
        existing_data = [[]]
        error_message = 'Exception During Loding JSON File'
        send_mail_or_logging(error_message, e)
        return existing_data

# Method for dump json data in file
def save_json(file_path, data):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
    except Exception as e:
        error_message = 'Exception During Saving Data in JSON File'
        send_mail_or_logging(error_message, e)

def add_new_case_entry(case_data, case_file):
    try:
        case_file_data = load_json(case_file)
        case_data["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if case_file_data != [[]]:
            case_file_data_list = case_file_data[0][0]["cases"]

            # search for existing case_id
            found = False
            for case in case_file_data_list:
                if case.get("case_id") == case_data.get("case_id"):
                    case.update(case_data)
                    found = True
                    break

            if not found:
                case_file_data_list.append(case_data)

            save_json(case_file, case_file_data)
        else:
            # First case entry
            payload_case_list = [case_data]
            case_file_data[0].append({'cases': payload_case_list})
            save_json(case_file, case_file_data)

    except Exception as e:
        error_message = 'Exception During Add New Case Entry in JSON File'
        send_mail_or_logging(error_message, e)

# Method for remove old case entry
def remove_old_case_entry(file_list):
    try:
        while True:
            cutoff_date = datetime.now() - timedelta(days=10)

            for payload_file in file_list:
                try:
                    payload_file_data = load_json(payload_file)
                    if payload_file_data != [[]]:
                        new_case_list = []

                        json_data_list = payload_file_data[0][0]["cases"]
                        for case in json_data_list:
                            if datetime.strptime(case["timestamp"], '%Y-%m-%d %H:%M:%S') > cutoff_date:
                                new_case_list.append(case)

                        payload_file_data[0][0]["cases"] = new_case_list

                        with open(payload_file, 'w') as f:
                            json.dump(payload_file_data, f, indent=4)

                except Exception as inner_e:
                    error_message = f'Error cleaning old entries for file: {payload_file}'
                    send_mail_or_logging(error_message, inner_e)

            time.sleep(86400)  # 1 day delay

    except Exception as e:
        error_message = 'Error in remove_old_case_entry loop'
        send_mail_or_logging(error_message, e)

# ✅ 5. UNIDIRECTION
def continuous_receiving_data(connection, initial_byte):
    """Continuously receive data from the machine."""
    global IS_RECIEVING_DATA
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
        global IS_RECIEVING_DATA
        if file:
            write_data_to_file(file, byte_array)
            logger.info(f'Data written to file: {file.name}')
            file.close()
        byte_array = []
        IS_RECIEVING_DATA = False

    try:
        logger.info("Started unidirectional reading...")

        # Process the initial byte if provided
        if initial_byte == b'\x05':
            try:
                handle_start_of_data(initial_byte)
            except Exception as e:
                send_mail_or_logging('Exception in open file', e)
            
        while IS_RECIEVING_DATA and connection:
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

# ✅ 6. BIDIRECTION 
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

def create_case_in_machine(connection, case_no, test_id, sample_type, sample_type_number):
    """Send commands to the machine to create a case."""
    
    try:
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
        
    except Exception as e:
        error_message = f"[CREATE CASE] Error in create case in machine, Case id: '{case_no}'"
        send_mail_or_logging(error_message, e)
        return {"status": "error", "message": str(e)}


# ✅ 7. MySQL Pending Case Handling 
def insert_backup_to_db(case_id, machine_id, payload):
    try:
        print("insert_backup_to_db")
        conn = get_connection()
        qry = """INSERT INTO pending_cases_backup (case_id, machine_id, payload) VALUES (%s, %s, %s)"""
        with conn.cursor() as cur:
            cur.execute( qry, (case_id, machine_id, json.dumps(payload)))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[BACKUP] Failed to insert case_id={case_id}", exc_info=False)

def cleanup_old_backup_from_db(interval_sec=86400, retention_days=10):
    try:
        logger.info("Pending retry thread started.\n")
        while True:
            try:
                print("cleanup_old_backup_from_db")
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(f"""
                        DELETE FROM pending_cases_backup
                        WHERE created_at < NOW() - INTERVAL {int(retention_days)} DAY
                    """)
                    deleted = cur.rowcount
                conn.commit()
                conn.close()
                logger.info(f"[BACKUP] 🧹 Deleted {deleted} backup entries older than {retention_days} days.")
            except Exception as e:
                logger.error("[BACKUP] Cleanup failed", exc_info=False)
            finally:
                time.sleep(interval_sec)  # 1 day wait
    except Exception as e:
        error_message = 'Error in remove old log'
        send_mail_or_logging(error_message, e)

def is_case_in_pending_cases(case_no):
    """Check if the case is already in the pending DB."""
    try:
        print("is_case_in_pending_cases")
        conn =get_connection()
        if not conn:
            logger.error("[CREATE CASE] DB connection not available in is_case_in_pending_cases")
            return False
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM pending_cases WHERE case_id = %s AND machine_id = %s"
        cursor.execute(query, (case_no, MACHINE_ID))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        send_mail_or_logging("[CREATE CASE] Error in is_case_in_pending_cases", e)
        return False  

def insert_pending_case_to_db(case_id, test_id, sample_type, sample_type_number, machine_id):
     
    try:
        print("insert_pending_case_to_db")
        conn =get_connection()
        if not conn:
            logger.error("[CREATE CASE] DB connection not available in insert_pending_case_to_db")
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
    except Exception as e:
        send_mail_or_logging("[CREATE CASE] Error inserting into pending_cases", e)
        
def delete_pending_case(case_id_db):
    try:
        print("")
        conn =get_connection()
        cursor = conn.cursor()
        del_query = "DELETE FROM pending_cases WHERE case_id = %s AND machine_id = %s"
        cursor.execute(del_query, (case_id_db, MACHINE_ID))
        conn.commit()
        cursor.close()
    except Exception as e:
        send_mail_or_logging(f"[CREATE CASE] DB deletion error for case ID {case_id_db} on machine {MACHINE_ID}", e)
    finally:
        if conn:
            conn.close()

RETRY_IN_PROGRESS = set()

def check_and_retry_pending_cases(connection):
    global IS_PENDING_RETRY_RUNNING, MONITORING_ACTIVE, IS_RECIEVING_DATA
  
    if IS_PENDING_RETRY_RUNNING:
        logger.info("[CREATE CASE] Retry already running, skipping.")
        return []
    elif IS_RECIEVING_DATA:
        logger.info("[CREATE CASE] Skipping retry — BI or UNI is active.")
        return []
    else:
        MONITORING_ACTIVE = False
        IS_PENDING_RETRY_RUNNING = True
    time.sleep(0.1)        # Optional
    processed_cases = []
        
    try: 
        logger.info("\n")
        logger.info(f"[CREATE CASE] {'=' * 100}")
        logger.info(f"[CREATE CASE] 🕒 Starting new retry cycle")
        print("check_and_retry_pending_cases")
        conn = get_connection()
        if conn is None:
            logger.error("[CREATE CASE] ⚠ DB connection failed. Will retry in 1 minute.")
            time.sleep(60)
            return []
        
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pending_cases where machine_id = %s", (MACHINE_ID,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows:
            logger.info("[CREATE CASE]  No pending cases found. ")
            return []

        for row in rows:
            print(row)
            case_id = row[1]
            
            if case_id in RETRY_IN_PROGRESS:
                logger.info(f"[CREATE CASE] Skipping case {case_id} — already being retried.")
                continue
            RETRY_IN_PROGRESS.add(case_id)
            
            try:
                case_entry = {
                    "case_id": row[1],
                    "test_id": row[2],
                    "sample_type": row[3],
                    "sample_type_number": row[4],
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                response = create_case_in_machine(
                    connection,
                    case_no=row[1],
                    test_id=row[2],
                    sample_type=row[3],
                    sample_type_number=row[4]
                )

                if response.get("status") == "success":
                    add_new_case_entry(case_entry, COMPLETE_CASE_FILE)
                    delete_pending_case(row[1])
                    processed_cases.append(row[1])                    
                else:
                    add_new_case_entry(case_entry, ERROR_CASE_FILE)
                    logger.error(f"[CREATE CASE] Pending Retry failed for Case '{row[1]}': {response.get('message')}. ❌")

            except Exception as e:
                send_mail_or_logging(f"[CREATE CASE] Error processing case {row[1]}.⚠️", e)
            finally:
                RETRY_IN_PROGRESS.discard(case_id)
                
        return processed_cases
    except Exception as e:
        send_mail_or_logging("[CREATE CASE] Error in retry pending case thread", e)

    finally:
            IS_PENDING_RETRY_RUNNING = False
            MONITORING_ACTIVE = True


# ✅ 8. Background Threads Setup
def schedule_pending_retry(connection):
    global IS_RECIEVING_DATA
    time.sleep(10)
    while True:
        try:
            if IS_RECIEVING_DATA:
                logger.info("[CREATE CASE]" + "=" * 100)
                logger.info("[CREATE CASE] ⏸ Retry skipped — BI/UNI active")
                logger.info("[CREATE CASE]" + "=" * 100 + "\n")
                time.sleep(60)
                continue
                
            # Count pending rows safely
            count = 0
            try:
                print("*****")
                conn = get_connection()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM pending_cases WHERE machine_id = %s",
                            (MACHINE_ID,)
                        )
                        count = cur.fetchone()[0]
                    conn.close()
            except Exception as e:
                logger.error("[CREATE CASE] DB count check failed", exc_info=False)
            print(count)
            if count > 0:
                result = check_and_retry_pending_cases(connection)
                logger.info(f"[CREATE CASE] {len(result)} Case id: {result} successfully processed and removed from DB. ✔️")
                logger.info(f"[CREATE CASE] {'=' * 100}\n")

        except Exception as e:
            send_mail_or_logging("[CREATE CASE] Error in scheduled pending retry", e)

        finally:
            time.sleep(150)
 
def checking_api_calling_and_data_reading(connection):
    """Continuously monitor for data from machine - starts on startup"""
    global IS_RECIEVING_DATA, MONITORING_ACTIVE
    logger.info("Started continuous monitoring for machine data...")
    while True: 
        try:
            if MONITORING_ACTIVE :
                try:
                    byte = connection.read()
                    print(byte)
                    if byte and byte != b'':
                        IS_RECIEVING_DATA = True
                        continuous_receiving_data(connection, byte)
                        
                    elif byte == generate_checksum.ACK.encode():
                        print("Received ACK from machine")
                
                except Exception as e:
                    if "timed out" not in str(e).lower():
                        send_mail_or_logging('Exception while monitoring data', e)
                        
            else:
                if IS_PENDING_RETRY_RUNNING:
                    # logger.info("Monitoring paused - Pending retry cases in progress")
                    pass
                time.sleep(0.5)
                
        except Exception as e:
            send_mail_or_logging('Critical exception in monitoring loop', e)
            time.sleep(5) 
            
        finally:
            IS_RECIEVING_DATA = False

        time.sleep(0.1)  
         
# Start monitoring thread on startup
def start_monitoring():
    """Start the monitoring thread"""
    try:
        monitoring_thread = threading.Thread(target=checking_api_calling_and_data_reading, args=(connection_c_311_2,), daemon=True,name="MonitoringThread")
        monitoring_thread.start()
        logger.info("Monitoring thread started")
        
        retry_thread = threading.Thread(target=schedule_pending_retry,args=(connection_c_311_2,), daemon=True,name="PendingRetryThread")
        retry_thread.start()
        logger.info("Pending retry thread started.\n")

    except Exception as e:
        error_message = 'Failed to establish machine connection'
        send_mail_or_logging(error_message, e)

# ✅ 9. FastAPI App Setup - API routes
@app.post(f"/create_case/{HOST_COM_PORT}")
async def api_create_case(request: CreateCaseRequest):
    """API endpoint to create a case."""
    try:
        logger.info(f"[BI-API] API called for case creation: '{request.case_id}'")
        if request.sample_type == "S1":
            sample_type_number = 1
        elif request.sample_type == "S2":
            sample_type_number = 2
        elif request.sample_type == "S3":
            sample_type_number = 3
        elif request.sample_type == "S4":
            sample_type_number = 4
        else:
            sample_type_number = 1
        
        # Insert case into pending_cases table in DB instead of immediate background task
        insert_pending_case_to_db(
            case_id=request.case_id,
            test_id=request.test_id,
            sample_type=request.sample_type,
            sample_type_number=sample_type_number,
            machine_id=request.machine_id
        )
        payload = request.model_dump()
        insert_backup_to_db(request.case_id, request.machine_id, payload)
        # logger.info(f"[BI-API] API called for case creation: '{request.case_id}' and Saved to DB ")
        return {"status": 200, "statusState": "success", "message": "Case queued for creation"}
        
    except Exception as e:
        error_message = 'Exception in api call'
        send_mail_or_logging(error_message, e)
        
        raise HTTPException(
            status_code=400,
            detail={
                "status": 400,
                "statusState": "error",
                "message": f"Case Creation Error: {e}"
            }
        )

# ✅ 10. Main
if __name__ == "__main__":
    try:
        logger.info(f"Starting FastAPI server on {HOST_ADDRESS}:{HOST_PORT}")
        
        # Start monitoring before starting the server
        start_monitoring()

        threading.Thread(target=remove_old_case_entry, args=(CASE_FILE_LIST,), daemon=True).start()
        threading.Thread(target=remove_old_logs_from_log_file, args=(LOG_FILE_LIST,), daemon=True).start()
        threading.Thread(target=cleanup_old_backup_from_db, args=(86400, 1), daemon=True, name="clean_old_backup_from_db").start()

        # Start the FastAPI server
        uvicorn.run(app, host=HOST_ADDRESS, port=HOST_PORT)
        
    except Exception as e:
        error_message = 'Exception in main function'
        send_mail_or_logging(error_message, e)