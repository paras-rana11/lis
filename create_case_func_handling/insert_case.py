import mysql.connector
import random
import string
from datetime import datetime

# MySQL connection config
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root1',
    'database': 'lis'
}

# Function to generate a unique random case_id
def generate_next_case_id(cursor):
    cursor.execute("SELECT case_id FROM pending_cases ORDER BY id DESC LIMIT 1")
    last = cursor.fetchone()
    if last:
        try:
            last_num = int(last[0].replace("case", ""))
            next_case_id = f"case{last_num + 1}"
        except ValueError:
            next_case_id = "case1"
    else:
        next_case_id = "case1"
    return next_case_id

# Insert a random case into the pending_cases table
def insert_random_case():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    case_id = generate_next_case_id(cursor)
    test_id = f"TEST{random.randint(1000, 9999)}"
    sample_type = random.choice(['Blood', 'Urine', 'Swab', 'Saliva'])
    sample_type_number = random.randint(1, 10)
    machine_id = f"MCH-{random.randint(100, 999)}"

    try:
        cursor.execute("""
            INSERT INTO pending_cases (case_id, test_id, sample_type, sample_type_number, machine_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (case_id, test_id, sample_type, sample_type_number, machine_id))

        conn.commit()
        print(f"✅ Case inserted: {case_id}")

    except mysql.connector.IntegrityError:
        print("⚠️ Duplicate case_id, trying again...")
        insert_random_case()  # Retry with a new case_id

    finally:
        cursor.close()
        conn.close()

for _ in range(10):
    insert_random_case()

