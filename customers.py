import pandas as pd
import psycopg2

# Step 1: Extract - Read data from CSV file
csv_file_path = 'files/customers.csv'
df = pd.read_csv(csv_file_path)

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        dbname="Jewelry",
        user="postgres",
        password="VladoS_44",
        host="localhost"
    )

    # Step 2: Load - Insert or update data in the customer table
    cursor = connection.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Customers (FirstName,LastName,Email,Password,Phone,Address,RegistrationDate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Email) DO UPDATE SET
                FirstName = EXCLUDED.FirstName,
                LastName = EXCLUDED.LastName,
                Password = EXCLUDED.Password,
                Phone = EXCLUDED.Phone,
                Address= EXCLUDED.Address,
                RegistrationDate = EXCLUDED.RegistrationDate
        """, (row['FirstName'], row['LastName'], row['Email'], row['Password'], row['Phone'], row['Address'], row['RegistrationDate']))

    # Commit the transaction
    connection.commit()

except Exception as error:
    print(f"Error: {error}")
    if connection:
        connection.rollback()

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()

print("Data import completed successfully.")