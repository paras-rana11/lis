patients_dict = {
    'P123': {'HbA1c': '6.2', 'new' : '9.0'}
}

zipped_tests = {'Glucose': '110', 'HbA1c': '6.5'}

# Now update:
patients_dict['P123'].update(zipped_tests)

print("updated patient dict: ", patients_dict)


patients_dict = {
    'P123': {'HbA1c': '6.2'}
}

zipped_tests = {'Glucose': '110', 'HbA1c': '6.5'}

# Now update:
patients_dict['P123'] = zipped_tests

print("= patient dict: ", patients_dict)