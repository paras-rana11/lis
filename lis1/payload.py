LAB_BRANCH_ID = '001'

results = [
    [('MISPA_NANO', '7777', "{'ALBUMIN^ALBUMIN^^F': '4.174265', '^^^F': '35.565537', '^BILIRUBIN DIRECT^^F': '0.151548', '^BILIRUBIN TOTAL^^F': '0.758605', '^Calcium A^^F': '9.918960', '^CHOLESTEROL^^F': '154.311925', '^ENZYMATIC CREATININE^^F': '1.203981', '^HDL DIRECT^^F': '49.407390', 'SGPT^SGPT AGAPPE^^F': '20.366606', '^TRIGLYCERIDES^^F': '86.489728', '^TOTAL PROTIEN^^F': '7.068528', '^URIC ACID^^F': '8.305209'}")],
    [('MISPA_NANO', '8888', "{'ALBUMIN^ALBUMIN^^F': '4.174265', '^^^F': '35.565537', '^BILIRUBIN DIRECT^^F': '0.151548', '^BILIRUBIN TOTAL^^F': '0.758605', '^Calcium A^^F': '9.918960', '^CHOLESTEROL^^F': '154.311925', '^ENZYMATIC CREATININE^^F': '1.203981', '^HDL DIRECT^^F': '49.407390', 'SGPT^SGPT AGAPPE^^F': '20.366606', '^TRIGLYCERIDES^^F': '86.489728', '^TOTAL PROTIEN^^F': '7.068528', '^URIC ACID^^F': '8.305209'}")]
]


# payload for send data to healthray function
output_dict = {
    'patient_case_results': [
        {
            'lab_branch_id': LAB_BRANCH_ID,
            'lis_machine_id': sub_item[0],
            'patient_case_no': sub_item[1],
            'result': sub_item[2]
        }
        for item in results for sub_item in item
    ]
}

print(output_dict)


# output_dict :
{
    'patient_case_results': 
        [
            {
                'lab_branch_id': '001', 'lis_machine_id': 'MISPA_NANO', 'patient_case_no': '7777', 'result': "{'ALBUMIN^ALBUMIN^^F': '4.174265', '^^^F': '35.565537', '^BILIRUBIN DIRECT^^F': '0.151548', '^BILIRUBIN TOTAL^^F': '0.758605', '^Calcium A^^F': '9.918960', '^CHOLESTEROL^^F': '154.311925', '^ENZYMATIC CREATININE^^F': '1.203981', '^HDL DIRECT^^F': '49.407390', 'SGPT^SGPT AGAPPE^^F': '20.366606', '^TRIGLYCERIDES^^F': '86.489728', '^TOTAL PROTIEN^^F': '7.068528', '^URIC ACID^^F': '8.305209'}"
            }, 
            {
                'lab_branch_id': '001', 'lis_machine_id': 'MISPA_NANO', 'patient_case_no': '8888', 'result': "{'ALBUMIN^ALBUMIN^^F': '4.174265', '^^^F': '35.565537', '^BILIRUBIN DIRECT^^F': '0.151548', '^BILIRUBIN TOTAL^^F': '0.758605', '^Calcium A^^F': '9.918960', '^CHOLESTEROL^^F': '154.311925', '^ENZYMATIC CREATININE^^F': '1.203981', '^HDL DIRECT^^F': '49.407390', 'SGPT^SGPT AGAPPE^^F': '20.366606', '^TRIGLYCERIDES^^F': '86.489728', '^TOTAL PROTIEN^^F': '7.068528', '^URIC ACID^^F': '8.305209'}"
            }
         ]
}
