#This pipeline cleans the columns of the product_list.xlsx file

# Issues to fix:
# 1. Duplicated product IDs with different product_names
# 2. Null value in the product_type column
# 3. Unnecessary columns (first column)

import pandas as pd;
import os

# Load the product_list.xlsx file
base_dir = os.path.dirname(os.path.realpath(__file__))

file_path = os.path.join(base_dir, '../../../Raw Dataset/Business Department/product_list.xlsx')
product_list = pd.read_excel(file_path)

# ------------------------------------------------------------------------------------- 

# Create a lookup for the old product_id and new product_ids
products_lookup = product_list[['product_id','product_name','price']].copy()

# Sort the values by increasing product IDs (closest approach to ensure that the products are in true chronological order)
products_lookup = products_lookup.sort_values(by='product_id')

# Create a 'new_id' column that assigns chronological product IDs
products_lookup['new_id'] = ['PRODUCT{:05d}'.format(i) for i in range(1,len(product_list)+1)]


# Create a new dataframe to join the original table with the lookup table
updated_product_list = product_list.merge(
    products_lookup[['product_id', 'product_name','new_id']],
    left_on=['product_id','product_name'],
    right_on=['product_id','product_name'],
    how='left'
)

# Change the current product IDs to the new IDs
updated_product_list['product_id'] = updated_product_list['new_id']

# Sort the new table by product ID
updated_product_list = updated_product_list.sort_values(by='product_id')

# Drop the new IDs column 
updated_product_list = updated_product_list.drop(columns=['new_id'])

# --------------------------------------------------------------------
# Filling the missing value for product_type
updated_product_list['product_type'] = updated_product_list['product_type'].fillna('Unspecified')

# Remove the unnecessary first column
updated_product_list = updated_product_list.drop(updated_product_list.columns[0],axis=1)

# ----------------------------------------------------------------------

# Writing the transformed files to a new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_product_list.csv')
updated_product_list.to_csv(file_path,index=False)




