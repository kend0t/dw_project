import pandas as pd
import os

import mysql.connector

# Load all data with order IDs
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Operations Department/New Files/cleaned_order_data.csv')
order_data = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Operations Department/New Files/cleaned_line_item_data_products.csv')
line_item_data_products = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Operations Department/New Files/cleaned_line_item_data_prices.csv')
line_item_data_prices = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Operations Department/New Files/cleaned_order_delays.csv')
delays = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Marketing Department/New Files/cleaned_transactional_campaign_data.csv')
campaign = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Enterprise Department/New Files/cleaned_order_with_merchant_list.csv')
order_with_merchants = pd.read_csv(file_path)

# ----------------------------------------------------------------

# Merge line_item_data_products and line_item_data_prices
# Since each row from both data has same IDs, we will just pair each row together
combined = line_item_data_products.merge(
    line_item_data_prices,
    left_index=True,
    right_index=True,
    how="left"
)

# Drop the order_id_y column
combined = combined.drop(columns={'order_id_y'})

# Change order_id_x column name to order_id
combined = combined.rename(columns={'order_id_x':'order_id'})

# Merge current data frame with order data
combined = combined.merge(
    order_data,
    left_on="order_id",
    right_on="order_id",
    how="left"
)

# Merge current data frame with campaign transactional data
combined = combined.merge(
    campaign,
    left_on=['order_id','transaction_date','estimated arrival'],
    right_on=['order_id','transaction_date','estimated arrival'],
    how="left"
)

# Fill in null values for campaign_id and availed columns
combined['campaign_id'] = combined['campaign_id'].fillna('CAMPAIGN00000')
combined['availed'] = combined['availed'].fillna('Not Applicable')

# Combined current data frame with order delays data
combined = combined.merge(
    delays,
    left_on=["order_id"],
    right_on=["order_id"],
    how="left"
    
)
# Merge current data frame with merchant and staff data
combined = combined.merge(
    order_with_merchants,
    left_on=["order_id"],
    right_on=["order_id"],
    how="left"
    
)
# Fill null values in delay in days
combined['delay in days'] = combined['delay in days'].fillna(-1)

# Drop other columns no follow fact table schema
product_sales_fact = combined.drop(columns=['estimated arrival', 'delay in days', 'price','product_name']).copy()

# Sort rows by transaction date
product_sales_fact = product_sales_fact.sort_values(by='transaction_date')

# Create a list of increasing sales id
product_sale_ids= ['PS{:07d}'.format(i) for i in range(1, len(product_sales_fact) + 1)]

# Insert product sale ids at first column
product_sales_fact.insert(0,'product_sale_id',product_sale_ids)


# -------------------------------------------------------------------------------------------------------------

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="kendot",
    database="project"
)

cursor = connection.cursor()

table_name = 'product_sales'

for _, row in product_sales_fact.iterrows():
    insert_query = f'''
        INSERT INTO {table_name} (product_sale_id, order_id, product_id, merchant_id, staff_id, campaign_id, transaction_date, availed, quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        
        ON DUPLICATE KEY UPDATE
        order_id = VALUES(order_id),
        product_id = VALUES(product_id),
        merchant_id = VALUES(merchant_id),
        staff_id= VALUES(staff_id),
        campaign_id = VALUES(campaign_id),
        transaction_date= VALUES(transaction_date),
        availed = VALUES(availed),
        quantity = VALUES(quantity),
        subtotal = VALUES(subtotal),
        final_total = VALUES(final_total);
    
    '''
    # Insert values from the row
    cursor.execute(insert_query, (row['product_id'], row['order_id'], row['product_id'], row['merchant_id'], row['staff_id'], row['campaign_id'],row['transaction_date'],row['availed'],row['quantity']))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data inserted successfully!")









