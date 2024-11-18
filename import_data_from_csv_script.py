import pandas as pd
import psycopg2

# PostgreSQL connection details
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "Data_analysis_project_COVID",
    "user": "postgres",
    "password": "beshoy123"
}

# Path to the CSV file
csv_file = "E:\data analysis\project\Data_analysis_project_COVID\CovidVaccinations.csv"

def infer_pgsql_dtype_from_column(column):
    """
    Infers the most appropriate PostgreSQL data type for a column based on its content.
    """
    is_integer = True
    is_bigint = False
    is_numeric = True
    is_boolean = True

    for value in column.dropna():
        value_str = str(value).strip().lower()

        # Check for boolean
        if value_str not in ["true", "false"]:
            is_boolean = False

        # Check for integer
        try:
            int_value = int(value)
            if not (-2_147_483_648 <= int_value <= 2_147_483_647):
                is_bigint = True
                is_integer = False
        except ValueError:
            is_integer = False

        # Check for numeric
        try:
            float(value)
        except ValueError:
            is_numeric = False

        # If none of the above, it's text
        if not is_boolean and not is_integer and not is_numeric:
            return "TEXT"

    # Prioritize data type in this order: BIGINT > INTEGER > NUMERIC > BOOLEAN
    if is_bigint:
        return "BIGINT"
    elif is_integer:
        return "INTEGER"
    elif is_numeric:
        return "NUMERIC"
    elif is_boolean:
        return "BOOLEAN"
    else:
        return "TEXT"

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to the database.")

    # Load CSV into a pandas DataFrame
    df = pd.read_csv(csv_file)
    table_name = "CovidVaccinations"

    # Infer PostgreSQL column types based on column content
    column_types = {}
    for column in df.columns:
        column_types[column] = infer_pgsql_dtype_from_column(df[column])

    # Create table query dynamically
    create_table_query = f"CREATE TABLE {table_name} ("
    for column, dtype in column_types.items():
        create_table_query += f"{column} {dtype}, "
    create_table_query = create_table_query.rstrip(", ") + ");"

    # Execute table creation query
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")  # Drop table if it exists
    cursor.execute(create_table_query)
    print(f"Table {table_name} created with the following columns and types:")
    for column, dtype in column_types.items():
        print(f"  - {column}: {dtype}")

    # Insert data into the table
    for _, row in df.iterrows():
        columns = ', '.join(df.columns)
        values = ', '.join(
            [
                f"'{str(x).replace('\'', '\'\'')}'" if column_types[col] in ["TEXT", "BOOLEAN"]
                else str(x) if pd.notnull(x) else "NULL"
                for col, x in zip(df.columns, row)
            ]
        )
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
        cursor.execute(insert_query)

    # Commit changes
    conn.commit()
    print("Data inserted successfully.")

except Exception as e:
    print("Error:", e)

finally:
    # Close the database connection
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed.")