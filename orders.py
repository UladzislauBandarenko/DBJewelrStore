import traceback
import pandas as pd
import psycopg2

# Step 1: Extract - Read data from CSV file
csv_file_path = 'files/orders.csv'
df = pd.read_csv(csv_file_path)


# Step 2: Transform
def get_id_mapping(table_name, id_col, name_col, connection):
    cursor = connection.cursor()
    cursor.execute(f"SELECT {id_col}, {name_col} FROM {table_name}")
    records = cursor.fetchall()
    cursor.close()
    return {record[1]: record[0] for record in records}


try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        dbname="Jewelry",
        user="postgres",
        password="VladoS_44",
        host="localhost"
    )


    # Step 2.1: Get Category and ID mappings
    customer_mapping = get_id_mapping("Customers", "CustomerID", "Email", connection)
    product_mapping = get_id_mapping("Products", "ProductID", "ArticleNumber", connection)
    order_mapping = get_id_mapping("Orders", "OrderID", "OrderNumber", connection)

    # Prepare the DataFrame with ID mappings
    df['CustomerID'] = df['Email'].map(customer_mapping)
    df['ProductID'] = df['ArticleNumber'].map(product_mapping)
    # df['OrderID'] = df['OrderNumber'].map(order_mapping)

    # Step 3: Load - Insert or update data in the product table
    cursor = connection.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Orders (OrderDate, CustomerID, TotalAmount, Status, OrderNumber)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (OrderNumber) DO UPDATE SET
                OrderDate = EXCLUDED.OrderDate,
                CustomerID = EXCLUDED.CustomerID,
                TotalAmount= EXCLUDED.TotalAmount,
                Status = EXCLUDED.Status
            RETURNING OrderID
        """, (row['OrderDate'], row['CustomerID'], row['TotalAmount'], row['Status'], row['OrderNumber']))

        OrderID = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO OrderItems (OrderID, ProductID, Quantity, OrderNumber)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (OrderNumber, ProductID) DO UPDATE SET
                Quantity = EXCLUDED.Quantity
        """, (OrderID, row['ProductID'], row['Quantity'], row['OrderNumber']))

    connection.commit()

except Exception as error:
    print(f"Error: {error}")
    print("Detailed traceback:")
    traceback.print_exc()  # This prints the complete traceback to the console
    if connection:
        connection.rollback()

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()

print("Data import completed successfully.")
