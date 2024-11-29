import pandas as pd
import os

# Load the cleaned product_list file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Marketing Department/New Files/cleaned_campaign_data.csv')
campaign_data = pd.read_csv(file_path)

campaign_data = campaign_data.drop(columns="old_id")

print(campaign_data)