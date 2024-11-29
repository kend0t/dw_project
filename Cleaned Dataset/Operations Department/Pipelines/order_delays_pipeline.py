import pandas as pd
import os


base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_delays.html')
html_tables = pd.read_html(file_path)
orderdelays = pd.concat(html_tables, ignore_index=True)


# drop 'Unnamed: 0' column
orderdelays = orderdelays.drop('Unnamed: 0', axis=1)

# -----------------------------------------------------------------------------
# Writing the transformed files to a new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_order_delays.csv')
orderdelays.to_csv(file_path,index=False)