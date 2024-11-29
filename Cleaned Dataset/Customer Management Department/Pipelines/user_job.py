# This pipeline cleans the user_job.csv file

# Issue to fix:
# 1. Make sure IDs are consistent with changes to user_data.json
# 2. job_level column have null values
# 3. Remove unnecessary first column

import pandas as pd;
import os

# Load the cleaned_user_data.csv file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../New Files/cleaned_user_data.csv')
user_data = pd.read_csv(file_path)

# Load the user_job.csv file
file_path = os.path.join(base_dir, '../../../Raw Dataset/Customer Management Department/user_job.csv')
user_job = pd.read_csv(file_path)

# ----------------------------------------------------------------------------------------
# Create a lookup table for the new user ids
user_lookup = user_data[['old_id','user_id','name']].copy()
# Rename user_id column to new_id column
user_lookup.rename(columns={'user_id':'new_id'},inplace=True)

# Create new dataframe to join original table with lookup table
updated_user_job = user_job.merge(
    user_lookup[['old_id','new_id','name']],
    left_on=['user_id','name'],
    right_on=['old_id','name'],
    how='left'
)

# Change user IDs to new user IDs
updated_user_job['user_id'] = updated_user_job['new_id']
# Sort table in ascending user IDs
updated_user_job = updated_user_job.sort_values(by='user_id')
# Drop old_id and new_id columns
updated_user_job = updated_user_job.drop(columns=['new_id','old_id'])

# ----------------------------------------------------------------------
# Fill null values in job_level column with N/A
updated_user_job['job_level'] = updated_user_job['job_level'].fillna('Not Applicable')

# Drop unnecessary first column
updated_user_job = updated_user_job.drop(updated_user_job.columns[0], axis=1)

# -----------------------------------------------------------------------
# Writing the transformed files to a new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_user_job.csv')
updated_user_job.to_csv(file_path,index=False)
