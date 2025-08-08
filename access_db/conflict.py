import threading
import random
import string
import time
from mysql.connector import connect, Error

try:
    conn = connect(
        host="localhost",
        user="root",
        password="root1",  # change this
        database="temp"         # change this
    )
    print("✅ Global connection established (UNSAFE)")
except Exception as e:
    print(f"❌ Failed to connect to DB: {e}")
    exit(1)

# =============================
# FUNCTION TO GENERATE RANDOM CASE DATA
# =============================
def generate_random_case():
    case_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    machine_id = random.choice(["CobasC311", "SysmexXN1000", "MindrayBC"])
    data = f"Test data {random.randint(100, 999)}"
    return case_id, machine_id, data

# =============================
# INSERT FUNCTION (USES GLOBAL conn)
# =============================
def insert_random_case():
    case_id, machine_id, data = generate_random_case()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO pending_cases (case_id, machine_id, data) VALUES (%s, %s, %s)",
                       (case_id, machine_id, data))
        conn.commit()
        print(f"[INSERT] ✅ Case ID: {case_id}")
    except Error as e:
        print(f"[INSERT] ❌ Error: {e}")

# =============================
# DELETE FUNCTION (USES GLOBAL conn)
# =============================
def delete_oldest_case():
    try:
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
    print("🚀 Starting UNSAFE global connection test...\n")

    # Change number of threads and iterations to stress test
    insert_threads = [threading.Thread(target=insert_loop, args=(20,)) for _ in range(10)]
    delete_threads = [threading.Thread(target=delete_loop, args=(20,)) for _ in range(10)]

    for t in insert_threads + delete_threads:
        t.start()

    for t in insert_threads + delete_threads:
        t.join()

    print("\n❌ TEST COMPLETE: You just used a shared global connection. Expect errors in logs above.")
