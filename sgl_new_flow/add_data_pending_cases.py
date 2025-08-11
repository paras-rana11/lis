import mysql.connector
import random

# DB connection info
db_config = {
    'host': 'localhost',      # change as needed
    'user': 'root',
    'password': 'root1',
    'database': 'lis'
}

sample_types = ['serum', 'plasma', 'whole_blood', 'urine']
machines = ['C_311_1', 'BS_240', 'UNKNOWN']

def generate_test_id():
    # Randomly join 1 to 4 test ids like t1//t24//t3
    tests = [f't{random.randint(1, 30)}' for _ in range(random.randint(1, 4))]
    return '//'.join(tests)

def insert_random_case(cursor):
    case_id = f"CASE{random.randint(1000,9999)}"
    test_id = generate_test_id()
    sample_type = random.choice(sample_types)
    sample_type_number = sample_types.index(sample_type) + 1
    machine_id = random.choice(machines)

    sql = """
    INSERT INTO pending_cases (case_id, test_id, sample_type, sample_type_number, machine_id)
    VALUES (%s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        test_id = new.test_id,
        sample_type = new.sample_type,
        sample_type_number = new.sample_type_number,
        machine_id = new.machine_id;
    """
    vals = (case_id, test_id, sample_type, sample_type_number, machine_id)
    cursor.execute(sql, vals)
    print(f"Inserted/Updated: {case_id}")

def main():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Insert 10 random cases
    for _ in range(30):
        insert_random_case(cursor)
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
