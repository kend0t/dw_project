import pandas as pd
import os
pd.set_option('display.max_colwidth', None) # No truncation for column values
pd.set_option('display.expand_frame_repr', False)

base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../New Files/cleaned_campaign_data.csv')
campaign_data = pd.read_csv(file_path)

file_path = os.path.join(base_dir, '../../../Raw Dataset/Marketing Department/transactional_campaign_data.csv')
transactional_campaign_data = pd.read_csv(file_path)

# ----------------------------------------------------------------------------------------
# Create a lookup table for the new campaign ids

campaign_lookup = campaign_data[['old_id', 'campaign_id']].copy()
campaign_lookup.rename(columns={'campaign_id':'new_id'},inplace=True)

# Create new dataframe to join original table with lookup table
updated_transactional_campaign = transactional_campaign_data.merge(
    campaign_lookup[['old_id','new_id']],
    left_on=['campaign_id'],
    right_on=['old_id'],
    how='left'
)

# Change old campaign ids to the new campaign id and drop both new id and old id in the transaction table
updated_transactional_campaign['campaign_id'] = updated_transactional_campaign['new_id']
updated_transactional_campaign = updated_transactional_campaign.drop(columns=['new_id','old_id'])

# Create a new transaction id column, using the sorted data by transaction date as reference 
updated_transactional_campaign = updated_transactional_campaign.sort_values(by='transaction_date')
updated_transactional_campaign['transaction_id'] = ['TRANSACTION{:05d}'.format(i) for i in range(1,len(transactional_campaign_data)+1)]

# Drop the original index column
updated_transactional_campaign = updated_transactional_campaign.drop(updated_transactional_campaign.columns[0], axis=1)

# Reorganize the table columns
updated_transactional_campaign = updated_transactional_campaign[['transaction_id', 'campaign_id', 'order_id', 'transaction_date', 'estimated arrival', 'availed']]

# Write the transformed file to new csv file
file_path = os.path.join(base_dir, '../New Files/cleaned_transactional_campaign_data.csv')
updated_transactional_campaign.to_csv(file_path,index=False)