import pandas as pd
import os

# Load the 
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../../Customer Management Department/New Files/cleaned_user_data.csv')
user_data = pd.read_csv(file_path)

# Load the user_credit_card_.pickle file
file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_data_20200101-20200701.parquet')
orderData1 = pd.read_parquet(file_path)

file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_data_20200701-20211001.pickle')
orderData2 = pd.read_pickle(file_path)

file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_data_20211001-20220101.csv')
orderData3 = pd.read_csv(file_path)

file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_data_20221201-20230601.json')
orderData4 = pd.read_json(file_path)

file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_data_20220101-20221201.xlsx')
orderData5 = pd.read_excel(file_path)

file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/order_data_20230601-20240101.html')
html_tables = pd.read_html(file_path)
orderData6 = pd.concat(html_tables, ignore_index=True)

# ----------------------------------------------------------------------------------------
# Merge all order_data files
order = [orderData1, orderData2, orderData3, orderData4, orderData5, orderData6]
allorderdata = pd.concat(order)

# drom 'Unnamed: 0' column
allorderdata = allorderdata.drop('Unnamed: 0', axis=1)

# transform estimated arrival = transaction_date + estimated arrival > datetime format
allorderdata['transaction_date'] = pd.to_datetime(allorderdata['transaction_date'])
allorderdata['estimated arrival'] = allorderdata['estimated arrival'].str.extract(r'(\d+)').astype(int)
allorderdata['estimated arrival'] = allorderdata['transaction_date'] + pd.to_timedelta(allorderdata['estimated arrival'], unit='D')

# ----------------------------------------------------------------------------------------
# Create a lookup table for the new user ids
user_lookup = user_data[['old_id', 'user_id']].drop_duplicates(subset='old_id').copy()
# Rename user_id column to new_id column
user_lookup.rename(columns={'user_id':'new_id'},inplace=True)

# Create a new dataframe to join the original table with the lookup table
updated_order_data = allorderdata.merge(
    user_lookup[['old_id','new_id']],
        left_on=['user_id'],
    right_on=['old_id'],
    how='left'
)

# Change user IDs to new user IDs
updated_order_data['user_id'] = updated_order_data['new_id']

# Sort by transactiondate
updated_order_data = updated_order_data.sort_values('transaction_date')

# Drop new_id and old_id columns
updated_order_data = updated_order_data.drop(columns=['new_id','old_id'])

# -----------------------------------------------------------------------------
# Writing the transformed files to a new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_order_data.csv')
updated_order_data.to_csv(file_path,index=False)