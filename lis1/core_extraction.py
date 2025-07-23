import os

os.chdir(".")

file_path = "C:\\practice\\LIS\\lis1\\report_file\\mispa_nano_2024-10-01_09-52-41.txt"

def read_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        print("\nError while reading file: ", e)
        return False
    
def extract_from_mispa_nano():
    machine_name = 'MISPA_NANO'
    patient_id = None
    patients = []
    patient_dict = {}
    
    data = read_file(file_path)
    
    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17', '\x1c', '\x0b']

    for sym in astm_symbols:
        data = data.replace(sym, '')
    
    lines = data.strip().split('\n')
    cleaned_lines = []
    temp_line = ""

    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith(('P|', 'O|', 'R|', 'L|')):
            if i+1 < len(lines) and not lines[i+1].strip().startswith(('P|', 'O|', 'R|', 'L|')):
                merged_line = f"{line}{lines[i+1].strip()}"
                cleaned_lines.append(merged_line)
                i += 2
            else:
                cleaned_lines.append(line)        
        else:
            cleaned_lines.append(line)   
        i += 1
     
    print([line for line in cleaned_lines])
        
    for line in cleaned_lines:
        parts = line.strip().split("|")
        
        if line.startswith("O"):
            patient_id = parts[3].strip()
            if patient_id not in patient_dict:
                patient_dict[patient_id] = {}

        
        if line.startswith("R"):
            test_name = parts[2].strip()
            test_value = parts[3].strip().split("^")[0]

            patient_dict[patient_id][test_name] = test_value
                
    for pid, test in patient_dict.items():
        patients.append((machine_name, pid, str(test)))
            
    return patients
            
    
result = extract_from_mispa_nano()

print("\n\n==> FINAL OUTPUT: ")
print(result)
# for res in result:
#     print("=> ", res)