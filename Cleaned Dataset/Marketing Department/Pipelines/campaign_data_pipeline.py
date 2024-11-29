# This pipeline cleans the campaign_data.csv file

# Issue to fix:
# 1. The original CSV is missing comma as separation for values
# 2. Discount values not standardized
# 3. Campaign Name to be cleaned

import pandas as pd
import csv
import os

pd.set_option('display.max_colwidth', None) # No truncation for column values
pd.set_option('display.expand_frame_repr', False)

# Load the campaign_data.csv file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../../../Raw Dataset/Marketing Department/campaign_data.csv')

# ISSUE: campaign_data.csv is missing commas, all the records are currently stored in a single column...
# FIX: replace tabspace with comma
modified_csv = []

with open(file_path, mode='r') as file:
    # Read entire file content and replace tabs
    content = file.read().replace('\t', ',')  # Replace tabs globally with commas
    
    # Parse the modified content as CSV
    csvFile = csv.reader(content.splitlines())  # Splitlines keeps line structure intact
    for lines in csvFile:
        modified_csv.append(','.join(lines))  # Join fields with a comma

# Write back the modified CSV content
with open(file_path, mode='w') as file:
    file.write("\n".join(modified_csv) + "\n")

# Import and read csv files
campaign_data = pd.read_csv(file_path)

# Create copy of original csv data
modified_campaign_data = campaign_data.copy()

# CAMPAIGN DATA: Standardize discount values
modified_campaign_data["discount"] = modified_campaign_data["discount"].str.replace('[^\d+]', '', regex=True)
modified_campaign_data["discount"] = modified_campaign_data["discount"].astype('float').round(2)
modified_campaign_data["discount"] = modified_campaign_data["discount"]/100

# CAMPAIGN DATA CHANGE: Capitalize first letter of campaign name
modified_campaign_data["campaign_name"] = modified_campaign_data["campaign_name"].str.title().replace(to_replace=r"(')([A-Z])", value='\'t' ,regex=True)

file_path = os.path.join(base_dir, '../New Files/cleaned_campaign_data.csv')
modified_campaign_data.to_csv(file_path,index=False)

# Load cleaned_campaign_data.csv file
campaign_data = pd.read_csv(file_path)

# Create lookup for old campaign ID and new campaign ID
campaign_lookup = campaign_data[['campaign_id','campaign_name']].copy()

# Sort value by campaign id (To maintain chronological-placement)
campaign_lookup = campaign_lookup.sort_values(by='campaign_id')
campaign_lookup['new_id'] = ['CAMPAIGN{:05d}'.format(i) for i in range(1,len(campaign_data)+1)]

# Create new dataframe to join original table with lookup table
updated_campaign_data = campaign_data.merge(
    campaign_lookup[['campaign_id','campaign_name','new_id']],
    left_on=['campaign_id','campaign_name'],
    right_on=['campaign_id','campaign_name'],
    how='left'
)

# Replace old id with original id and campaign id with new id
updated_campaign_data['old_id'] = updated_campaign_data['campaign_id']
updated_campaign_data['campaign_id'] = updated_campaign_data['new_id']

# Sort by campaign id and remove the new id column
updated_campaign_data = updated_campaign_data.sort_values(by='campaign_id')
updated_campaign_data = updated_campaign_data.drop(columns=['new_id'])

# Remove the original index column
updated_campaign_data = updated_campaign_data.drop(updated_campaign_data.columns[0], axis=1)

# Load to cleaned_campaign_data csv
updated_campaign_data.to_csv(file_path,index=False)

