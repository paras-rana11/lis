import time
import mysql.connector
from datetime import datetime
import logging
from random import randint




logger = logging.getLogger("retry_worker")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("case_retry_worker.log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)




def create_case_in_machine(connection, case_no, test_id, sample_type, sample_type_number):
    """
    Simulated version of the machine interaction logic.
    In actual code, replace this with the real command sequence.
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
            return {"status": "error", "message": "Machine did not respond"}
    except Exception as e:
        return {"status": "error", "message": str(e)}



def add_case_to_json(case_entry, machine_id, filename):
    """
    Save successful case to local JSON file.
    Optional for your need.
    """
    import os
    import json

    # file_path = os.path.join("C:\\ASTM\\root\\case_file\\", filename)
    # os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    file_path = 'complete_case.json'

    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                data = json.load(f)
        else:
            data = {}

        if machine_id not in data:
            data[machine_id] = []

        data[machine_id].append(case_entry)

        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)

        logger.info(f"Added case to JSON: {case_entry['case_id']}")
    except Exception as e:
        logger.error(f"Failed to write JSON: {e}")


def send_mail_or_logging(message, error):
    logger.error(f"{message} - {error}")



def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root1",
            database="lis"
        )
        return conn
    except Exception as e:
        send_mail_or_logging("DB connection failed", e)
        return None



def retry_pending_cases():
    while True:
        try:
            logger.info("\n" + "="*100)
            logger.info(" Starting new retry cycle")
            logger.info("="*100)
            
            conn = get_db_connection()
            if conn is None:
                logger.warning("⚠ DB connection failed. Will retry in 1 minute.")
                time.sleep(60)
                continue

            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM pending_cases")
            rows = cursor.fetchall()
            
            if not rows:
                logger.info(" No pending cases found.")
            else:
                logger.info(f" Found {len(rows)} pending cases.")

            for row in rows:
                try:
                    logger.info("="*80)
                    logger.info(f" Retrying case: {row['case_id']}")
                    logger.info("="*80)
                    
                    response = create_case_in_machine(
                        connection=None,
                        case_no=row["case_id"],
                        test_id=row["test_id"],
                        sample_type=row["sample_type"],
                        sample_type_number=row["sample_type_number"]
                    )

                    if response.get("status") == "success":
                        case_entry = {
                            "case_id": row["case_id"],
                            "test_id": row["test_id"],
                            "sample_type": row["sample_type"],
                            "sample_type_number": row["sample_type_number"],
                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        add_case_to_json(case_entry, row["machine_id"], 'complete_case_creation.json')

                        cursor.execute("DELETE FROM pending_cases WHERE id = %s", (row["id"],))
                        conn.commit()
                        logger.info(f" Case {row['case_id']} successfully processed and removed from DB.")
                    else:
                        logger.warning(f" Retry failed for Case {row['case_id']}: {response.get('message')}")

                except Exception as e:
                    send_mail_or_logging(f"Error processing case {row['case_id']}", e)

            cursor.close()
            conn.close()

        except Exception as e:
            send_mail_or_logging("Error in retry_pending_cases", e)

        logger.info(" Waiting 1 minute before next retry cycle...\n")
        time.sleep(60)


# if __name__ == "__main__":
#     retry_pending_cases()























