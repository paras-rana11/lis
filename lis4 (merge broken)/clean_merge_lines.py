import os
os.chdir(".")

def clean_and_merge_broken_lines(data):

    control_chars = ['\x02', '\x03', '\x04', '\x05', '\x06', '\x15', '\x17', '\x1c', '\x0b']
    for sym in control_chars:
        data = data.replace(sym, '')

    lines = data.replace('\r', '').split('\n')

    cleaned_lines = []
    current_line = ""
    known_prefixes = ('MSH|', 'PID|', 'OBR|', 'OBX|', 'DSC|')

    for line in lines:
        line = line.strip()

        if line.startswith(known_prefixes):
            if current_line:
                cleaned_lines.append(current_line)
            current_line = line
        else:
            current_line += " " + line

    # Add the last segment
    if current_line:
        cleaned_lines.append(current_line)

    return cleaned_lines


file_path = 'C:\\All\\LIS\\lis4 (merge broken)\\fincare1.txt'

with open(file_path, 'r') as f:
    data = f.read()
    
result = clean_and_merge_broken_lines(data)

print("\n====> Final Result: ")
for r in result:
    print("\nR: ", r)

