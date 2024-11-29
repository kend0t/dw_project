import pandas as pd
import os

base_dir = os.path.dirname(os.path.realpath(__file__))

merchant_data_path = os.path.join(base_dir, "../../../Raw Dataset/Enterprise Department/merchant_data.html")
merchant_data = pd.read_html(merchant_data_path, match='merchant_id', flavor="html5lib")
merchant_data = merchant_data[0]

# para sure na datetime format
merchant_data["creation_date"] = pd.to_datetime(merchant_data["creation_date"])

# reorder table by creation_date
merchant_data = merchant_data.sort_values(by="creation_date").reset_index(drop=True)

# creating a new merchant id column
merchant_data['new_merchant_id'] = ['MERCHANT{:05d}'.format(i) for i in range(1, len(merchant_data) + 1)]

# standardizing contact_number
merchant_data["contact_number"] = merchant_data["contact_number"].replace(r"\(", "", regex=True).replace(r"\)", "-", regex=True).replace(r"\.", "-", regex=True)

# drop unnecesarry columns
merchant_data.drop(columns=["Unnamed: 0"], inplace=True)

# reorder columns
merchant_data = merchant_data[["merchant_id", "new_merchant_id", "name", "contact_number", "street", "state", "city", "country", "creation_date"]]

# convert to csv
merchant_data_file_path = os.path.join(base_dir, '../New Files/cleaned_merchant_data_list.csv')
merchant_data.to_csv(merchant_data_file_path, index=False)

print(merchant_data.head())