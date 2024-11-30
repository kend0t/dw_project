import pandas as pd
import os


base_dir = os.path.dirname(os.path.realpath(__file__))

# reading staff_data.html
staff_data_path = os.path.join(base_dir, '../../../Raw Dataset/Enterprise Department/staff_data.html')
staff_data = pd.read_html(staff_data_path, flavor="html5lib")
staff_data = staff_data[0]

# convert creation_date data type
staff_data["creation_date"] = pd.to_datetime(staff_data["creation_date"])

# reorder table by creation_date
staff_data = staff_data.sort_values(by="creation_date").reset_index(drop=True)

# create new ids
staff_data['new_staff_id'] = ['STAFF{:05d}'.format(i) for i in range(1, len(staff_data) + 1)]

# standardizing contact_number
staff_data["contact_number"] = staff_data["contact_number"].replace(r"\(", "", regex=True).replace(r"\)", "-", regex=True).replace(r"\.", "-", regex=True)

# drop unnesecary colunmns
staff_data.drop(columns=["Unnamed: 0"], inplace=True)

# reorder columns
staff_data = staff_data[["staff_id", "new_staff_id", "name", "job_level", "contact_number", "street", "state", "city", "country", "creation_date"]]

# convert to csv
staff_data_file_path = os.path.join(base_dir, '../New Files/cleaned_staff_data_list.csv')
staff_data.to_csv(staff_data_file_path, index=False)

print(staff_data.head())