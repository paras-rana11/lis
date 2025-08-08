import threading
import random
import string
import time
from mysql.connector import connect, Error
from datetime import datetime

# --- DB CONFIG --- #
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root1",
    "database": "temp"
}

# --- DB CONNECTION PER THREAD --- #
def get_connection():
    return connect(**DB_CONFIG)

# --- HELPER FUNCTIONS --- #
def generate_random_case():
    case_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    machine_id = random.choice(["CobasC311", "SysmexXN1000", "MindrayBC"])
    data = f"Test data {random.randint(100, 999)}"
    return case_id, machine_id, data

# --- INSERT FUNCTION --- #
def insert_random_case():
    case_id, machine_id, data = generate_random_case()
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO pending_cases (case_id, machine_id, data) VALUES (%s, %s, %s)",
                       (case_id, machine_id, data))
        conn.commit()
        print(f"[INSERT] ✅ Case ID: {case_id}")
    except Error as e:
        print(f"[INSERT] ❌ Error: {e}")
    finally:
        if conn:
            conn.close()

# --- DELETE FUNCTION --- #
def delete_oldest_case():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT case_id FROM pending_cases ORDER BY created_at ASC LIMIT 1")
        row = cursor.fetchone()
        if row:
            case_id = row[0]
            cursor.execute("DELETE FROM pending_cases WHERE case_id = %s", (case_id,))
            conn.commit()
            print(f"[DELETE] 🗑️ Deleted case_id: {case_id}")
        else:
            print("[DELETE] ❗ No case to delete")
    except Error as e:
        print(f"[DELETE] ❌ Error: {e}")
    finally:
        if conn:
            conn.close()

# --- THREAD TARGET --- #
def insert_loop(n):
    for _ in range(n):
        insert_random_case()
        time.sleep(random.uniform(0.1, 0.5))

def delete_loop(n):
    for _ in range(n):
        delete_oldest_case()
        time.sleep(random.uniform(0.2, 0.6))

# --- MAIN DRIVER --- #
if __name__ == "__main__":
    insert_threads = [threading.Thread(target=insert_loop, args=(10,)) for _ in range(5)]  # 5 threads x 10 inserts
    delete_threads = [threading.Thread(target=delete_loop, args=(10,)) for _ in range(5)]  # 5 threads x 10 deletes

    # Start all threads
    for t in insert_threads + delete_threads:
        t.start()

    # Wait for all to finish
    for t in insert_threads + delete_threads:
        t.join()

    print("\n✅ Test complete. Check DB for final state.")
