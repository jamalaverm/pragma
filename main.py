import os
import re
from path import PROJECT_PATH
from orchestrator import Orchestrator

if __name__ == '__main__':
    # Read configuration file
    config_path = os.path.join(PROJECT_PATH, "config.ini")
    orc = Orchestrator(config_path)

    # Iterate over multiple files, use regexp to avoid reading validation.csv
    files_path = os.path.join(PROJECT_PATH, "files")
    for dir_path, _, file_names in os.walk(files_path):
        for file in file_names:
            if re.search(r'\d{4}-\d{1,2}.csv', file):
                file_path = os.path.join(dir_path, file)
                print("Current file:", file)
                orc.insert_file(file_path)
