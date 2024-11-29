import pandas as pd
import os


base_dir = os.path.dirname(os.path.realpath(__file__))

merchant_data_path = os.path.join(base_dir, "../../../Raw Dataset/Enterprise Department/merchant_data.html")
merchant_data = pd.read_html(merchant_data_path, match='merchant_id', flavor="html5lib")
merchant_data = merchant_data[0]


# para sure na datetime format
merchant_data["creation_date"] = pd.to_datetime(merchant_data["creation_date"])

# creating a new merchant id column
merchant_data = merchant_data.sort_values(by="creation_date").reset_index(drop=True)
merchant_data['new_merchant_id'] = ['MERCHANT{:05d}'.format(i) for i in range(1, len(merchant_data) + 1)]

# standardizing contact_number
merchant_data["contact_number"] = merchant_data["contact_number"].replace(r"\(", "", regex=True).replace(r"\)", "-", regex=True).replace(r"\.", "-", regex=True)

# creating a mapping of old ids to new ids
merchant_id_mapping = {}
old_merchant_ids = merchant_data["merchant_id"].to_list()
new_merchant_ids = merchant_data["new_merchant_id"].to_list()

for old, new in zip(old_merchant_ids, new_merchant_ids):
    merchant_id_mapping[old] = new

# correcting old 
order_with_merchant1_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/order_with_merchant_data1.parquet')
order_with_merchant2_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/order_with_merchant_data2.parquet')
order_with_merchant3_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/order_with_merchant_data3.csv')

order_with_merchant1 = pd.read_parquet(order_with_merchant1_path)
order_with_merchant2 = pd.read_parquet(order_with_merchant2_path)
order_with_merchant3 = pd.read_csv(order_with_merchant3_path)

# correct foreign keys in order_with_merchant tables
order_with_merchant1["merchant_id"] = order_with_merchant1["merchant_id"].replace(merchant_id_mapping)
order_with_merchant2["merchant_id"] = order_with_merchant2["merchant_id"].replace(merchant_id_mapping)
order_with_merchant3["merchant_id"] = order_with_merchant3["merchant_id"].replace(merchant_id_mapping)

# dealing with staff data
staff_data_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/staff_data.html')
staff_data = pd.read_html(staff_data_path, flavor="html5lib")
staff_data = staff_data[0]

staff_data["creation_date"] = pd.to_datetime(staff_data["creation_date"])

staff_data = staff_data.sort_values(by="creation_date").reset_index(drop=True)

staff_data['new_staff_id'] = ['STAFF{:05d}'.format(i) for i in range(1, len(staff_data) + 1)]

staff_id_mapping = {}
old_staff_ids = staff_data["staff_id"].to_list()
new_staff_ids = staff_data["new_staff_id"].to_list()

for old, new in zip(old_staff_ids, new_staff_ids):
    staff_id_mapping[old] = new

order_with_merchant1["staff_id"] = order_with_merchant1["staff_id"].replace(staff_id_mapping)
order_with_merchant2["staff_id"] = order_with_merchant2["staff_id"].replace(staff_id_mapping)
order_with_merchant3["staff_id"] = order_with_merchant3["staff_id"].replace(staff_id_mapping)

# combine order_with_merchant_data
order_with_merchant_data = pd.concat([order_with_merchant1, order_with_merchant2, order_with_merchant3])

# drop unnecesarry columns
merchant_data.drop(columns=["Unnamed: 0", "merchant_id"], inplace=True)
order_with_merchant_data.drop(columns=["Unnamed: 0"], inplace=True)
staff_data.drop(columns=["Unnamed: 0", "staff_id"], inplace=True)

# rename id columns
# merchant_data = merchant_data.rename(columns={"new_merchant_id": "merchant_id"})
# staff_data = staff_data.rename(columns={"new_staff_id": "staff_id"})

# rename and reposition id columns
merchant_id_column = merchant_data.pop("new_merchant_id")
merchant_data.insert(0, "merchant_id", merchant_id_column)

staff_id_column = staff_data.pop("new_staff_id")
staff_data.insert(0, "staff_id", merchant_id_column)

merchant_data_file_path = os.path.join(base_dir, '../New Files/cleaned_merchant_list.csv')
order_with_merchant_file_path = os.path.join(base_dir, '../New Files/cleaned_order_with_merchant_list.csv')
staff_data_file_path = os.path.join(base_dir, '../New Files/cleaned_staff_data_list.csv')

merchant_data.to_csv(merchant_data_file_path, index=False)
order_with_merchant_data.to_csv(order_with_merchant_file_path, index=False)
staff_data.to_csv(staff_data_file_path, index=False)

print(merchant_data.head(), order_with_merchant_data.head(), staff_data.head(), sep="\n--------------------------------------------------------------------------------------\n")