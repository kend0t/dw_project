import pandas as pd
import os
import mysql.connector

# Load the cleaned merchant data file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Enterprise Department/New Files/cleaned_merchant_list.csv')
merchant_data = pd.read_csv(file_path)



# ---------------------------------------------------------------------------------------------------------
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kendot",
    database="project"
)

cursor = connection.cursor()

table_name = 'merchants'


for _, row in merchant_data.iterrows():
    insert_query = f'''
        INSERT INTO {table_name} (merchant_id, merchant_name, merchant_street, merchant_state, merchant_city, merchant_country, merchant_contact_number, merchant_creation_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        merchant_name = VALUES(merchant_name),
        merchant_street = VALUES(merchant_street),
        merchant_state = VALUES(merchant_state),
        merchant_city = VALUES(merchant_city),
        merchant_country = VALUES(merchant_country),
        merchant_contact_number = VALUES(merchant_contact_number),
        merchant_creation_date = VALUES(merchant_creation_date);
    '''
    # Insert values from the row
    cursor.execute(insert_query, (row['merchant_id'], row['name'], row['street'], row['state'], row['city'],row['country'], row['contact_number'], row['creation_date']))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data inserted successfully!")