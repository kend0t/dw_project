# This pipeline cleans the column of the user_data.json file

# Issues to fix:
# 1. Duplicate user IDs with different customer names

import pandas as pd;
import os

# Load the user_data.json file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../../../Raw Dataset/Customer Management Department/user_data.json')
user_data = pd.read_json(file_path)

# -----------------------------------------------------------------------

# Create a lookup for the old user IDs and new user IDs
user_lookup = user_data[['user_id','creation_date','name']].copy()

# Sort the values by creation date (Ensures that the user IDs are in true chronological order)
user_lookup = user_lookup.sort_values(by='creation_date')

# Create a new column 'new_id' that assigns chronological user IDs
user_lookup['new_id'] = ['USER{:05d}'.format(i) for i in range(1,len(user_data)+1)]

# Create a new dataframe to join the original table with the lookup table
updated_user_data = user_data.merge(
    user_lookup[['user_id','name','new_id']],
    left_on=['user_id','name'],
    right_on=['user_id','name'],
    how='left'
)

# Change the current user ID to the new user ID
updated_user_data['user_id'] = updated_user_data['new_id']

# Sort the new table by user IDs
updated_user_data = updated_user_data.sort_values(by='user_id')

# Drop the new IDs column
updated_user_data = updated_user_data.drop(columns=['new_id'])

# -----------------------------------------------------------------

# Writing the transformed files to a new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_user_data_.csv')
updated_user_data.to_csv(file_path,index=False)

