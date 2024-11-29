#
#
# NOTE: this should be run after merchant_data_pipeline.py and staff_data_pipeline.py 
# because it depends on their cleaned data
#
#

import pandas as pd
import os


base_dir = os.path.dirname(os.path.realpath(__file__))

# read merchant data
merchant_data_path = os.path.join(base_dir, "../../../Cleaned Dataset/Enterprise Department/New Files/cleaned_merchant_data_list.csv")
merchant_data = pd.read_csv(merchant_data_path)

# read staff data
staff_data_path = os.path.join(base_dir, '../../../Cleaned Dataset/Enterprise Department/New Files/cleaned_staff_data_list.csv')
staff_data = pd.read_csv(staff_data_path)

# create a mapping of old ids to new ids
merchant_id_mapping = {}
old_merchant_ids = merchant_data["merchant_id"].to_list()
new_merchant_ids = merchant_data["new_merchant_id"].to_list()

staff_id_mapping = {}
old_staff_ids = staff_data["staff_id"].to_list()
new_staff_ids = staff_data["new_staff_id"].to_list()

for old, new in zip(old_merchant_ids, new_merchant_ids):
    merchant_id_mapping[old] = new

for old, new in zip(old_staff_ids, new_staff_ids):
    staff_id_mapping[old] = new

# reading order_with_merchant data
order_with_merchant1_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/order_with_merchant_data1.parquet')
order_with_merchant2_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/order_with_merchant_data2.parquet')
order_with_merchant3_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/order_with_merchant_data3.csv')

order_with_merchant1 = pd.read_parquet(order_with_merchant1_path)
order_with_merchant2 = pd.read_parquet(order_with_merchant2_path)
order_with_merchant3 = pd.read_csv(order_with_merchant3_path)

# combine order_with_merchant_data
order_with_merchant_data = pd.concat([order_with_merchant1, order_with_merchant2, order_with_merchant3])

# correct foreign keys in order_with_merchant tables
order_with_merchant_data["merchant_id"] = order_with_merchant_data["merchant_id"].replace(merchant_id_mapping)
order_with_merchant_data["staff_id"] = order_with_merchant_data["staff_id"].replace(staff_id_mapping)

# drop unnecesarry columns
order_with_merchant_data.drop(columns=["Unnamed: 0"], inplace=True)
merchant_data.drop(columns=["merchant_id"], inplace=True)
staff_data.drop(columns=["staff_id"], inplace=True)

# rename and reposition id columns
merchant_id_column = merchant_data.pop("new_merchant_id")
merchant_data.insert(0, "merchant_id", merchant_id_column)

staff_id_column = staff_data.pop("new_staff_id")
staff_data.insert(0, "staff_id", staff_id_column)

# convert to csv and update merchant & staff csvs
order_with_merchant_data_file_path = os.path.join(base_dir, '../New Files/cleaned_order_with_merchant_data_list.csv')
merchant_data_file_path = os.path.join(base_dir, '../New Files/cleaned_merchant_data_list.csv')
staff_data_file_path = os.path.join(base_dir, '../New Files/cleaned_staff_data_list.csv')

order_with_merchant_data.to_csv(order_with_merchant_data_file_path, index=False)
merchant_data.to_csv(merchant_data_file_path, index=False)
staff_data.to_csv(staff_data_file_path, index=False)

print(merchant_data.head(), order_with_merchant_data.head(), staff_data.head(), sep="\n--------------------------------------------------------------------------------------\n")