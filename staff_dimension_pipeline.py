import pandas as pd
import os
import mysql.connector

# Load the staff data file
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Enterprise Department/New Files/cleaned_staff_data_list.csv')
staff_data = pd.read_csv(file_path)

# -----------------------------------------------------------
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kendot",
    database="project"
)

cursor = connection.cursor()

table_name = 'staff'


for _, row in staff_data.iterrows():
    insert_query = f'''
        INSERT INTO {table_name} (staff_id, staff_name, staff_job_level, staff_street, staff_state, staff_city, staff_country,staff_contact_number,staff_creation_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        
        ON DUPLICATE KEY UPDATE
        staff_name = VALUES(staff_name),
        staff_job_level = VALUES(staff_job_level),
        staff_street = VALUES(staff_street),
        staff_state = VALUES(staff_state),
        staff_city = VALUES(staff_city),
        staff_country = VALUES(staff_country),
        staff_contact_number= VALUES(staff_contact_number),
        staff_creation_date = VALUES(staff_creation_date);
        
    
    '''
    # Insert values from the row
    cursor.execute(insert_query, (row['staff_id'],  row['name'], row['job_level'], row['street'], row['state'], row['city'],row['country'], row['contact_number'], row['creation_date'],))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data inserted successfully!")