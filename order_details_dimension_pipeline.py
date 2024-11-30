import pandas as pd
from product_sales_fact_pipeline import combined
import mysql.connector

# Create a copy of combined order data from product_sales pipeline
order_details = combined[['order_id','estimated arrival','delay in days']].copy()
final_order_details = order_details.drop_duplicates(subset='order_id',keep='first')


# ----------------------------------------------------------------------------
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kendot",
    database="project"
)

cursor = connection.cursor()

table_name = 'order_details'


for _, row in final_order_details.iterrows():
    insert_query = f'''
        INSERT INTO {table_name} (order_id, estimated_arrival, delay_in_days)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
        estimated_arrival = VALUES(estimated_arrival),
        delay_in_days = VALUES(delay_in_days);
    '''
    # Insert values from the row
    cursor.execute(insert_query, (row['order_id'], row['estimated arrival'], row['delay in days']))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data inserted successfully!")



