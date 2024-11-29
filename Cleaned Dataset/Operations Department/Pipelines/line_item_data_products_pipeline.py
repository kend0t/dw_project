import pandas as pd
import os

# Load the 
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../../Business Deparment/New Files/cleaned_product_list.csv')
user_data = pd.read_csv(file_path)

#Load the line_item_data_products sources

file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/line_item_data_products1.csv')
product1 = pd.read_csv(file_path)
file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/line_item_data_products2.csv')
product2 = pd.read_csv(file_path)
file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/line_item_data_products3.parquet')
product3 = pd.read_parquet(file_path)


#merge all product files into one dataframe
prod = [product1, product2, product3]
allproduct = pd.concat(prod)

# drop 'Unnamed: 0' column
allproduct = allproduct.drop('Unnamed: 0', axis=1)

# Create a lookup table for the new user ids
product_lookup = user_data[['old_id','product_id','product_name']].copy()
# Rename user_id column to new_id column
product_lookup.rename(columns={'product_id':'new_id'},inplace=True)

# ----------------------------------------------------------------------------------------

# Create a new dataframe to join the original table with the lookup table
updated_product = allproduct.merge(
    product_lookup[['old_id','new_id','product_name']],
    left_on=['product_id','product_name'],
    right_on=['old_id','product_name'],
    how='left'
)

# Change user IDs to new user IDs
updated_product['product_id'] = updated_product['new_id']


# Drop new_id and old_id columns
updated_product = updated_product.drop(columns=['new_id','old_id'])


# -----------------------------------------------------------------------------

# make csv
file_path = os.path.join(base_dir, '../New Files/cleaned_line_item_data_products.csv')
updated_product.to_csv(file_path, index=False)