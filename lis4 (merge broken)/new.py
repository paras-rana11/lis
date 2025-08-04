import os
os.chdir(".")

def extract_data_from_fincare(data):
    # print(data)
    machine_name = 'FINCARE'
    patients = []
    patient_id = None
    patients_dict = {}
    
    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17', '\x1c', '\x0b']

    for sym in astm_symbols:
        data = data.replace(sym, '')
    
    lines = data.strip().split("\n")
    
    cleaned_lines = []    
    cur_line = ''
    
    for line in lines:
        line = line.strip()
        if line.startswith(('MSH|', 'PID|', 'OBR|', 'OBX|','DSC|')):
            if cur_line:
                cleaned_lines.append(cur_line)
            cur_line = line
        else:
            cur_line += " " + line
    
    if cur_line:
        cleaned_lines.append(cur_line)
                
    for line in cleaned_lines:
        parts = line.strip().split("|")
        if line.startswith('PID'):
            patient_id = parts[5].strip().replace(" ", '')
            
            if patient_id and patient_id not in patients_dict:
                patients_dict[patient_id] = {}
                
        if line.startswith('OBX'):
            print(parts)
            
            cleaned_test_names = parts[4].strip().split()
            raw_test_values = parts[5].strip().split()
            
            print(cleaned_test_names)
            print(raw_test_values)
            
            cleaned_test_values = []
            
            for i in range(0, len(raw_test_values), 2):
                cleaned_test_values.append(raw_test_values[i])

            cleaned_tests = dict(zip(cleaned_test_names, cleaned_test_values))   
        
            if patient_id in patients_dict:
                patients_dict[patient_id].update(cleaned_tests)
            else:
                patients_dict[patient_id] = cleaned_tests
                
    for pid , val in patients_dict.items():
            patients.append((machine_name, pid, str(val)))
            
    return patients
                
        
        
    
   
   
   

file_path = 'C:\\All\\LIS\\lis4 (merge broken)\\fincare1.txt'

with open(file_path, 'r') as f:
    data = f.read()
    
result = extract_data_from_fincare(data)

print("\n====> Final Result: \n")
for r in result:
    print("\nR: ", r)

