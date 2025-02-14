import psycopg2
import pandas as pd

# Database connection parameters
conn = psycopg2.connect(
    dbname="ecommrece_one",
    user="postgres",
    password="123123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Load processed e-commerce data
def load_ecommerce_data():
    # Load the processed CSV file
    df = pd.read_csv("cleande_data.csv")
    print("Number of rows in CSV:", len(df))
    print("Sample data from CSV:")
    print(df.head())

    # Insert into Customers table
    customers_df = df[['customer_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']].drop_duplicates()
    print("Number of rows to insert into Customers:", len(customers_df))
    for _, row in customers_df.iterrows():
        cursor.execute("""
            INSERT INTO Customers (customer_id, customer_zip_code_prefix, customer_city, customer_state)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """, (row['customer_id'], row['customer_zip_code_prefix'], row['customer_city'], row['customer_state']))

    # Insert into Orders table
    orders_df = df[['order_id', 'customer_id', 'order_purchase_timestamp', 'order_approved_at']].drop_duplicates()
    print("Number of rows to insert into Orders:", len(orders_df))
    for _, row in orders_df.iterrows():
        cursor.execute("""
            INSERT INTO Orders (order_id, customer_id, order_purchase_timestamp, order_approved_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (row['order_id'], row['customer_id'], row['order_purchase_timestamp'], row['order_approved_at']))

    # Insert into Products table
    products_df = df[['product_id', 'product_category_name', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']].drop_duplicates()
    print("Number of rows to insert into Products:", len(products_df))
    for _, row in products_df.iterrows():
        cursor.execute("""
            INSERT INTO Products (product_id, product_category_name, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING;
        """, (row['product_id'], row['product_category_name'], row['product_weight_g'], row['product_length_cm'], row['product_height_cm'], row['product_width_cm']))

    # Insert into Payments table
    payments_df = df[['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']].drop_duplicates()
    print("Number of rows to insert into Payments:", len(payments_df))
    for _, row in payments_df.iterrows():
        cursor.execute("""
            INSERT INTO Payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['order_id'], row['payment_sequential'], row['payment_type'], row['payment_installments'], row['payment_value']))

    # Insert into OrderItems table
    order_items_df = df[['order_id', 'product_id', 'seller_id', 'price', 'shipping_charges']].drop_duplicates()
    print("Number of rows to insert into OrderItems:", len(order_items_df))
    for _, row in order_items_df.iterrows():
        cursor.execute("""
            INSERT INTO OrderItems (order_id, product_id, seller_id, price, shipping_charges)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['order_id'], row['product_id'], row['seller_id'], row['price'], row['shipping_charges']))

try:
    # Call the function to load data
    load_ecommerce_data()

    # Commit the transaction
    conn.commit()
    print("Data loaded successfully!")

except Exception as e:
    # Rollback in case of error
    conn.rollback()
    print(f"Error: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Connection closed.")