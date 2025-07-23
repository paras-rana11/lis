

import os

os.chdir(".")
MACHINE_NAME = 'MINDRAY'

def extract_data_from_2(file_path):
    patients = []
    patient_dict = {}
    patient_id = None

    try:
        with open(file_path, 'r') as f:
            data = f.read()
    except Exception as e:
        print("Error while reading file:", e)
        return []

    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    data_blocks = data.strip().split('1H')

    print("data_blocks:", data_blocks)
    
    for block in data_blocks:
        print("\nblock:", block)
        lines = block.strip().split("\n")
        print("\nlines:", lines)

        joined_lines = []
        temp_line = ""
        for line in lines:
            # print("\n\n\n\n\n\n")
            if line.startswith(("R|", "O|", "P|", "L|")):
                # print('\nline.startswith(("R|", "O|", "P|", "L|")):', line)
                if temp_line:
                    # print("\n--- if temp_line in if block:", True, temp_line)
                    joined_lines.append(temp_line)
                    # print('\njoined_lines:', joined_lines)
                temp_line = line
                # print("\nif temp_line out of if block:", temp_line)
            else:
                temp_line += line.strip()
                # print("\nelse templine: ",temp_line)
                
        if temp_line:
            joined_lines.append(temp_line)
            print("temp_line joined: ",temp_line)

        for line in joined_lines:
            parts = line.strip().split("|")

            if line.startswith("O|"):
                patient_id = parts[3].strip()
                if patient_id and patient_id not in patient_dict:
                    patient_dict[patient_id] = {}
                

            elif line.startswith("R|") and patient_id:
                try:
                    test_name = parts[2]  
                    test_value = parts[3].split("^")[0].strip()

                    # test_name = test_info.split("^")[0].strip()
                    # value = test_value.split("^")[0].strip()

                    if test_name and test_value:
                        patient_dict[patient_id][test_name] = test_value
                except Exception as e:
                    print(f"Error parsing line: {line}\nReason: {e}")

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients

file_path = os.path.join(os.getcwd(), "2.txt")
result = extract_data_from_2(file_path)

for res in result:
    print("\n\n==> Result:\n", res)
