# This pipeline cleans the user_credit_card.json file

# Issues to fix:
# 1. Make user IDs consistent with changes to user_data.json

import pandas as pd
import os

# Load the cleaned_user_data.csv file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../New Files/cleaned_user_data.csv')
user_data = pd.read_csv(file_path)

# Load the user_credit_card_.pickle file
file_path = os.path.join(base_dir, '../../../Raw Dataset/Customer Management Department/user_credit_card.pickle')
user_credit_card = pd.read_pickle(file_path)

# ----------------------------------------------------------------------------------------
# Create a lookup table for the new user ids
user_lookup = user_data[['old_id','user_id','name']].copy()
# Rename user_id column to new_id column
user_lookup.rename(columns={'user_id':'new_id'},inplace=True)

# Create a new dataframe to join the original table with the lookup table
updated_user_credit_card = user_credit_card.merge(
    user_lookup[['old_id','new_id','name']],
    left_on=['user_id','name'],
    right_on=['old_id','name'],
    how='left'
)

# Change user IDs to new user IDs
updated_user_credit_card['user_id'] = updated_user_credit_card['new_id']

# Sort by ascending user IDs
updated_user_credit_card = updated_user_credit_card.sort_values('user_id')

# Drop new_id and old_id columns
updated_user_credit_card = updated_user_credit_card.drop(columns=['new_id','old_id'])

# -----------------------------------------------------------------------------
# Writing the transformed files to a new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_user_credit_card.csv')
updated_user_credit_card.to_csv(file_path,index=False)