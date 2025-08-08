line = "PID|1||PDL22-S^^^^MR||POTI^SHARD"

rerun = line.split('|')[3].split('^')[0]


raw_patientId = line.split('|')[3].replace('^MR', '').replace('^', '')

if '-' in raw_patientId:
    patientId = raw_patientId.split('-')[0] 
else:
    patientId = raw_patientId
    
print(rerun)
print(patientId)









































str1 = """
C644
C645
C646
C647
C648
"""