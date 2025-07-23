import os
os.chdir(".")

def extract_data_from_fincare(data):
    # print(data)
    Machine_Name = 'FINCARE'
    patients = []
    patient_id = None
    patients_dict = {}
    
    control_char = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x1C']
    
    for sym in control_char:
        data = data.replace(sym, '')
    
    
    lines = data.strip().split('\n')
    
    for line in lines:
        
            if not line:
                continue
            print(line)
            
            parts = line.strip().split('|')
            # print("parts: ", parts)
            if line.startswith('PID'):
                patient_id = parts[5].strip() 
                # print('patient_id: ', patient_id)
                
                if patient_id not in patients_dict:
                    patients_dict[patient_id] = {}
                    
            # if line.startswith('OBX'):
            #     for i in range(4, len(line)): #, 3):
            #         test_name = parts[i]
                    # test_val = parts[i+2]
                    # print("test_name: ", test_name)
                    # print("test_val: ", test_val)
                 
    
    




with open('fincare1.txt', 'r') as f:
    data = f.read()
    
result = extract_data_from_fincare(data)

print("\n====> Final Result: \n")
# for r in result:
#     print("\nR: ", r)


print('vt:', ord('♂'))
print('fs:', ord('∟'))