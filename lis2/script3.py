import os
import re

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

    # Include all relevant ASTM control characters
    astm_symbols = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17']
    for sym in astm_symbols:
        data = data.replace(sym, '')

    data_blocks = data.strip().split('1H')

    for block in data_blocks:
        lines = block.strip().split("\n")

        joined_lines = []
        temp_line = ""
        for line in lines:
            if line.startswith(("R|", "O|", "P|", "L|")):
                if temp_line:
                    joined_lines.append(temp_line)
                temp_line = line
            else:
                temp_line += line.strip()
        if temp_line:
            joined_lines.append(temp_line)

        for line in joined_lines:
            parts = line.strip().split("|")

            if line.startswith("O|"):
                try:
                    patient_id = parts[3].strip()
                    if patient_id and patient_id not in patient_dict:
                        patient_dict[patient_id] = {}
                except Exception as e:
                    print(f"⚠ Error reading patient ID: {e}")
                    continue

            elif line.startswith("R|") and patient_id:
                try:
                    test_info = parts[2].strip()
                    test_value = parts[3]

                    test_name = re.sub(r"\x17..", '', test_info)
                    test_name = re.sub(r"\x02", '', test_name)
                    value = test_value.split("^")[0].strip()

                    print(f"=> test_name: {test_name} \t=> value: {value}")

                    if test_name and value:
                        patient_dict[patient_id][test_name] = value

                except Exception as e:
                    print(f"⚠ Error parsing line: {line}\nReason: {e}")

    for pid, tests in patient_dict.items():
        patients.append((MACHINE_NAME, pid, str(tests)))

    return patients


file_path = os.path.join(os.getcwd(), "2.txt")
result = extract_data_from_2(file_path)

for res in result:
    print("\n\n==> Result:\n", res)
