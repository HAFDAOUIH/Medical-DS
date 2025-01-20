import pymysql

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='',  # Add your password if required
    database='healthcare_db'
)

try:
    # Get the list of tables and their columns
    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        table_columns = {}
        for table in tables:
            cursor.execute(f"SHOW COLUMNS FROM {table}")
            columns = [row[0] for row in cursor.fetchall()]
            table_columns[table] = columns

        # Print the results
        for table, columns in table_columns.items():
            print(f"Table: {table}")
            print(f"Columns: {', '.join(columns)}")
            print()

finally:
    connection.close()
