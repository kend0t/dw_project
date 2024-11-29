import pandas as pd
import os

# Load the staff data file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Enterprise Department/New Files/cleaned_staff_data_list.csv')
staff_data = pd.read_csv(file_path)

print(staff_data)