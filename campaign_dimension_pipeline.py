import pandas as pd
import os
import mysql.connector
# Load the cleaned product_list file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Marketing Department/New Files/cleaned_campaign_data.csv')
campaign_data = pd.read_csv(file_path)

campaign_data = campaign_data.drop(columns="old_id")

# ---------------------------------------------------------------------------------------------------------
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kendot",
    database="project"
)

cursor = connection.cursor()

table_name = 'campaigns'


for _, row in campaign_data.iterrows():
    insert_query = f'''
        INSERT INTO {table_name} (campaign_id, campaign_name, campaign_description, discount)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        campaign_name = VALUES(campaign_name),
        campaign_description = VALUES(campaign_description),
        discount = VALUES(discount);
    '''
    # Insert values from the row
    cursor.execute(insert_query, (row['campaign_id'], row['campaign_name'], row['campaign_description'], row['discount']))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data inserted successfully!")

