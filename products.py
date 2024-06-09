import traceback
import pandas as pd
import psycopg2

# Step 1: Extract - Read data from CSV file
csv_file_path = 'files/products.csv'
df = pd.read_csv(csv_file_path)


def get_id_mapping(table_name, id_col, name_col, connection):
    cursor = connection.cursor()
    cursor.execute(f"SELECT {id_col}, {name_col} FROM {table_name}")
    records = cursor.fetchall()
    cursor.close()
    return {record[1]: record[0] for record in records}


def upsert_category_and_suppliers(df, connection):
    cursor = connection.cursor()

# Upsert categories
    for category in df['Category'].unique():
        cursor.execute("""
            INSERT INTO Categories (CategoryName) VALUES (%s)
            ON CONFLICT (CategoryName) DO NOTHING
        """, (category,))

    # Upsert Suppliers
    for SuppliersContact, SupplierName, SupplierPhone, SupplierAddress in df[['ContactEmail', 'SupplierName', 'SupplierPhone', 'SupplierAddress']].dropna().drop_duplicates().values:
        cursor.execute("""
            INSERT INTO Suppliers (ContactEmail,  SupplierName, SupplierPhone, SupplierAddress) VALUES (%s, %s, %s, %s)
            ON CONFLICT (ContactEmail) DO NOTHING
        """, (SuppliersContact, SupplierName, SupplierPhone, SupplierAddress))

    connection.commit()
    cursor.close()

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        dbname="Jewelry",
        user="postgres",
        password="VladoS_44",
        host="localhost"
    )

    # Step 2.1: Upsert category and pet type data
    upsert_category_and_suppliers(df, connection)

    # Step 2.2: Get Category and ID mappings
    category_mapping = get_id_mapping("Categories", "CategoryID", "CategoryName", connection)
    suppliers_type_mapping = get_id_mapping("Suppliers", "SupplierID", "ContactEmail", connection)

    # Prepare the DataFrame with ID mappings
    df['CategoryID'] = df['Category'].map(category_mapping)
    df['SupplierID'] = df['ContactEmail'].map(suppliers_type_mapping)

    # Step 3: Load - Insert or update data in the product table
    cursor = connection.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Products (ProductName, Price, StockQuantity, ArticleNumber, CategoryID, SupplierID)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ArticleNumber) DO UPDATE SET
                ProductName = EXCLUDED.ProductName,
                Price = EXCLUDED.Price,
                StockQuantity= EXCLUDED.StockQuantity,
                CategoryID = EXCLUDED.CategoryID,
                SupplierID = EXCLUDED.SupplierID
        """, (row['ProductName'], row['Price'], row['StockQuantity'], row['ArticleNumber'], row['CategoryID'], row['SupplierID']))

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
