import psycopg2
import json
import argparse
import sys
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Load configuration from config.json
with open('config.json', 'r', encoding='utf-8') as f:
    tables_config = json.load(f)

# Fetch database variables from .env
DB_CONFIG = {
    'user': os.getenv('user'),
    'password': os.getenv('password'),
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'dbname': os.getenv('dbname')
}


def get_table_columns(table_name):
    """Get columns for a specific table from config.json"""
    for table_info in tables_config:
        if table_info['table'] == table_name:
            return table_info['columns']
    return None


def query_table(cursor, table_name):
    """Query data from a table"""
    columns = get_table_columns(table_name)
    if not columns:
        print(f"Error: Table '{table_name}' not found in config.json")
        return
    
    columns_str = ', '.join(columns)
    print(f"Querying table: '{table_name}'")
    print(f"Columns: {columns}")
    
    cursor.execute(f"SELECT {columns_str} FROM {table_name} LIMIT 10;")
    results = cursor.fetchall()
    print(f"Total rows fetched: {len(results)}")
    
    for row in results:
        row_dict = dict(zip(columns, row))
        print(f"  {row_dict}")
    print()


def add_data(cursor, connection, json_file, table_name):
    """Add data from json file to a table"""
    columns = get_table_columns(table_name)
    if not columns:
        print(f"Error: Table '{table_name}' not found in config.json")
        return
    
    # Load data from json file
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Prepare columns and values for INSERT
    insert_columns = []
    insert_values = []
    
    for col in columns:
        # Skip 'id' column (auto-generated)
        if col == 'id':
            continue
        
        # Get value from data, try both original and mapped key
        value = None
        if col in data:
            value = data[col]
        elif col in column_mapping and column_mapping[col] in data:
            value = data[column_mapping[col]]
        else:
            # Try reverse mapping
            for json_key, db_col in column_mapping.items():
                if db_col == col and json_key in data:
                    value = data[json_key]
                    break
        
        if value is not None:
            insert_columns.append(col)
            insert_values.append(value)
    
    columns_str = ', '.join(insert_columns)
    placeholders = ', '.join(['%s'] * len(insert_values))
    
    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    print(f"Inserting data into table: '{table_name}'")
    print(f"SQL: {sql}")
    print(f"Values: {insert_values}")
    
    cursor.execute(sql, insert_values)
    connection.commit()
    print("Data inserted successfully!")


def main():
    parser = argparse.ArgumentParser(description='Database test tool')
    parser.add_argument('--table', type=str, help='Table name to operate on')
    parser.add_argument('--add', type=str, metavar='JSON_FILE', help='JSON file containing data to add')
    
    args = parser.parse_args()
    
    if not args.table:
        print("Error: --table argument is required")
        parser.print_help()
        sys.exit(1)
    
    # Connect to the database
    try:
        connection = psycopg2.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['dbname']
        )
        print("Connection successful!\n")
        
        cursor = connection.cursor()
        
        if args.add:
            # Add mode: read from json file and insert
            add_data(cursor, connection, args.add, args.table)
        else:
            # Query mode: read table data
            query_table(cursor, args.table)
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("\nConnection closed.")
        
    except Exception as e:
        print(f"Failed to connect or execute: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
