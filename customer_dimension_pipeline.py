import pandas as pd
import os
import mysql.connector

# Load the cleaned customer management department files
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Customer Management Department/New Files/cleaned_user_data.csv')
user_data = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Customer Management Department/New Files/cleaned_user_job.csv')
user_job = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Customer Management Department/New Files/cleaned_user_credit_card.csv')
user_credit_card= pd.read_csv(file_path)

customer_dimension = user_data.merge(
    user_job,
    left_on=['user_id','name'],
    right_on=['user_id','name'],
    how='left'
)
customer_dimension = customer_dimension.merge(
    user_credit_card,
    left_on=['user_id','name'],
    right_on=['user_id','name'],
    how='left'
)

customer_dimension = customer_dimension.drop(columns=['old_id'])
 # -----------------------------------------------------------
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kendot",
    database="project"
)

cursor = connection.cursor()

table_name = 'customers'


for _, row in customer_dimension.iterrows():
    insert_query = f'''
        INSERT INTO {table_name} (customer_id, customer_creation_date, customer_name, customer_street, customer_state, customer_city, customer_country, customer_birthdate, customer_gender,customer_device_address, customer_type,customer_job_title,customer_job_level,customer_credit_card_number,  customer_issuing_bank)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        
        ON DUPLICATE KEY UPDATE
        customer_creation_date = VALUES(customer_creation_date),
        customer_name = VALUES(customer_name),
        customer_street = VALUES(customer_street),
        customer_state = VALUES(customer_state),
        customer_city = VALUES(customer_city),
        customer_country = VALUES(customer_country),
        customer_birthdate = VALUES(customer_birthdate),
        customer_gender = VALUES(customer_gender),
        customer_device_address = VALUES(customer_device_address),
        customer_type = VALUES(customer_type),
        customer_job_title = VALUES(customer_job_title),
        customer_job_level = VALUES(customer_job_level),
        customer_credit_card_number = VALUES(customer_credit_card_number),
        customer_issuing_bank = VALUES(customer_issuing_bank);
        
    
    '''
    # Insert values from the row
    cursor.execute(insert_query, (row['user_id'], row['creation_date'], row['name'], row['street'], row['state'], row['city'],row['country'],row['birthdate'],row['gender'],row['device_address'], row['user_type'], row['job_title'], row['job_level'], row['credit_card_number'], row['issuing_bank']))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data inserted successfully!")






