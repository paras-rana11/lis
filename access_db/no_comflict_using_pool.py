import threading
import random
import string
import time
from mysql.connector import pooling, Error

# =============================
# ✅ CONNECTION POOL SETUP
# =============================
try:
    dbconfig = {
        "host": "localhost",
        "user": "root",
        "password": "root1",  # change this
        "database": "temp"         # change this
    }

    pool = pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=20,  # number of reusable connections
        **dbconfig
    )

    print("✅ Connection pool created successfully.")

except Exception as e:
    print(f"❌ Failed to create connection pool: {e}")
    exit(1)

# =============================
# HELPER FUNCTION TO GET CONNECTION FROM POOL
# =============================
def get_connection():
    return pool.get_connection()

# =============================
# FUNCTION TO GENERATE RANDOM CASE DATA
# =============================
def generate_random_case():
    case_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    machine_id = random.choice(["CobasC311", "SysmexXN1000", "MindrayBC"])
    data = f"Test data {random.randint(100, 999)}"
    return case_id, machine_id, data

# =============================
# INSERT FUNCTION
# =============================
def insert_random_case():
    case_id, machine_id, data = generate_random_case()
    conn = None
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

# =============================
# DELETE FUNCTION
# =============================
def delete_oldest_case():
    conn = None
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

# =============================
# THREAD LOOPS
# =============================
def insert_loop(n):
    print("[THREAD] Insert thread started.")
    for _ in range(n):
        insert_random_case()
        time.sleep(random.uniform(0.05, 0.1))

def delete_loop(n):
    print("[THREAD] Delete thread started.")
    for _ in range(n):
        delete_oldest_case()
        time.sleep(random.uniform(0.05, 0.1))

# =============================
# MAIN DRIVER
# =============================
if __name__ == "__main__":
    print("🚀 Starting connection pool test...\n")

    # Modify thread count if needed
    insert_threads = [threading.Thread(target=insert_loop, args=(20,)) for _ in range(10)]
    delete_threads = [threading.Thread(target=delete_loop, args=(20,)) for _ in range(10)]

    for t in insert_threads + delete_threads:
        t.start()

    for t in insert_threads + delete_threads:
        t.join()

    print("\n✅ TEST COMPLETE: You used connection pooling. No global connection issues should occur.")
