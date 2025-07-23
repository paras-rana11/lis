import os

os.chdir('C:\\practice\\LIS\\lis3\\')

def extract_data_from_cobas_c_311(file_path):
    
    patient_id = None
    patients = []
    patients_dict = {}
    
    with open(file_path, 'r', encoding='latin1') as f:
        data = f.read()

    # Remove ASTM control characters
    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x17', '\x1c']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    # Fix: use splitlines to handle any newline type
    lines = data.strip().splitlines()

    print(f"\nTotal lines: {len(lines)}")

    for line in lines:
        if line.startswith("O|"):
            parts = line.strip().split("|")        
            print("\n[O] line:", line)
            print("parts:", parts)
        
        if line.startswith("R|"):
            parts = line.strip().split("|")        
            print("\n[R] line:", line)
            print("parts:", parts)

file_path = "C:\\practice\\LIS\\lis3\\cobas_c_311.txt"
print("\n\n=> Final Output: ")
extract_data_from_cobas_c_311(file_path)
