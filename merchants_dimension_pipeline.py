import pandas as pd
import os

# Load the cleaned merchant data file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Enterprise Department/New Files/cleaned_merchant_list.csv')
merchant_data = pd.read_csv(file_path)

print(merchant_data)