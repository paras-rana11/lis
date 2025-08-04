
import os
import time
import shutil


# Report file path
REPORT_FILE_PATH = 'C:\\ASTM\\root\\report_file\\'
BACKUP_FILE_PATH = 'C:\\ASTM\\root\\backup_file\\'
CASE_FILE_PATH = 'C:\\ASTM\\root\\case_file\\'


def read_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        return None


def f1(data, mid):
    print(f"f1 called with {data[0]}, and id: {mid}")
    return []



def backup_and_remove_file(file_path: str, backup_path: str):
    try:
        shutil.copy2(file_path, backup_path)
        os.remove(file_path)
        print(f"Backed up and removed file: {file_path}")
    except Exception as e:
        if 'The process cannot access the file because it is being used by another process' not in str(e):
            print('Error in removing or backing up file', e)



def process_all_files(folder_path, backup_path):
    try:
        final_results = []
        file_path = None
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            text_data = read_file(file_path)
            if not text_data:
                continue
            extractors = {
                'access_2_1': f1,
                'access_2_2': f1,
            }

            prefix, extractor = next(((prefix, ext) for prefix, ext in extractors.items() if filename.startswith(prefix)), None)
            print(prefix, extractor)
            if extractor:
                results = extractor(text_data, prefix)
                if isinstance(results, list):
                    for result in results:
                        if True:
                            backup_and_remove_file(file_path, backup_path)
                            final_results.append(result)
                           
            else:
                print(f"No extractor matched for file: {filename}")
                backup_and_remove_file(file_path, backup_path)
                
        return final_results
    except Exception as e:
        error_message = 'Error in extract data from txt file'
        print(error_message, e)
        return final_results


while True:
    try:
        all_results = process_all_files(REPORT_FILE_PATH, BACKUP_FILE_PATH)
        if all_results:
           
            for result in all_results:
                print(f"Processed case: {result}")
                print(f"Data Inserted :- {result}")
            time.sleep(2)

        time.sleep(8)
    except Exception as e:
        error_message = 'Error in main loop'
