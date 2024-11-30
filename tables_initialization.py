import mysql.connector

# Establish a connection to the MySQL database
connection = mysql.connector.connect(
    host="localhost", 
    user="root",  
    password="kendot",  
    database="project"  
)

cursor = connection.cursor()

# Creating tables
create_products_table = """
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(45) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_type VARCHAR(45),
    product_price FLOAT(7,2) NOT NULL
);
"""

create_customers_table = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(45) PRIMARY KEY,
    customer_creation_date DATETIME NOT NULL,
    customer_name VARCHAR(45) NOT NULL,
    customer_street VARCHAR(45),
    customer_state VARCHAR(45),
    customer_city VARCHAR(45),
    customer_country VARCHAR(255),
    customer_birthdate DATE,
    customer_gender VARCHAR(45),
    customer_device_address VARCHAR(255),
    customer_type VARCHAR(45),
    customer_job_title VARCHAR(45),
    customer_job_level VARCHAR(45),
    customer_credit_card_number VARCHAR(45),
    customer_issuing_bank VARCHAR(45)
);
"""

create_order_details_table = """
CREATE TABLE IF NOT EXISTS order_details (
    order_id VARCHAR(255) PRIMARY KEY,
    estimated_arrival DATE,
    delay_in_days INT DEFAULT -1
);
"""

create_merchants_table = """
CREATE TABLE IF NOT EXISTS merchants(
    merchant_id VARCHAR(45) PRIMARY KEY,
    merchant_name VARCHAR(45) NOT NULL,
    merchant_street VARCHAR(45),
    merchant_state VARCHAR(45),
    merchant_city VARCHAR(45),
    merchant_country VARCHAR(255),
    merchant_contact_number VARCHAR(45),
    merchant_creation_date DATETIME NOT NULL
);
"""

create_staff_table = """
CREATE TABLE IF NOT EXISTS staff(
    staff_id VARCHAR(45) PRIMARY KEY,
    staff_name VARCHAR(45) NOT NULL,
    staff_job_level VARCHAR(45),
    staff_street VARCHAR(45),
    staff_state VARCHAR(45),
    staff_city VARCHAR(45),
    staff_country VARCHAR(255),
    staff_contact_number VARCHAR(45),
    staff_creation_date DATETIME NOT NULL
);
"""

create_campaigns_table = """
CREATE TABLE IF NOT EXISTS campaigns(
    campaign_id VARCHAR(45) PRIMARY KEY,
    campaign_name VARCHAR(255),
    campaign_description VARCHAR(255),
    discount FLOAT
);
"""

create_product_sales_table = """
CREATE TABLE IF NOT EXISTS product_sales(
    product_sale_id VARCHAR(45) PRIMARY KEY,
    order_id VARCHAR(255),
    product_id VARCHAR(45),
    merchant_id VARCHAR(45),
    staff_id VARCHAR(45),
    campaign_id VARCHAR(45),
    transaction_date DATE NOT NULL,
    availed VARCHAR(45),
    quantity INT NOT NULL,
    subtotal FLOAT,
    final_total FLOAT,
    FOREIGN KEY (order_id) REFERENCES order_details(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
    FOREIGN KEY (staff_id) REFERENCES staff(staff_id),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);
"""

# Execute each statement separately
cursor.execute(create_products_table)
cursor.execute(create_customers_table)
cursor.execute(create_order_details_table)
cursor.execute(create_merchants_table)
cursor.execute(create_staff_table)
cursor.execute(create_campaigns_table)
cursor.execute(create_product_sales_table)

# Commit the changes to the database
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Tables created successfully!")
