import sqlite3
import os
from datetime import datetime
import pandas as pd

# Load CSV files into DataFrames
def insert_data(conn, repo_name):
    diem_backend_df = pd.read_csv(f'output/{repo_name}.csv')
    diem_backend_git_analysis_df = pd.read_csv(f'output/{repo_name}_git_analysis_result.csv')
    
    columns_to_drop = ['date', 'year', 'month', 'day', 'dir_1', 'dir_2', 'dir_3', 'dir_4']
    diem_backend_df.drop(columns=columns_to_drop, inplace=True)

    diem_backend_df.to_sql('commits', conn, if_exists='replace', index=False)
    diem_backend_git_analysis_df.to_sql('files', conn, if_exists='replace', index=False)

    # Commit and close the connection
    conn.commit()

def connect_to_db(db_name):
    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(db_name)
    return conn



# Function to get table schema
def get_table_schema(conn, table_name):
    schema_query = f"PRAGMA table_info({table_name})"
    schema_df = pd.read_sql(schema_query, conn)
    return schema_df


def export_structure(conn):
    # List of tables to get schema and data
    tables = ['commits', 'files']

    # Get schema 
    for table in tables:
        schema_df = get_table_schema(conn, table)
        
        # Save schema to CSV
        schema_df.to_csv(f'output/{table}_schema.csv', index=False)
        

    # Close the connection
    conn.close()



def fill_db(repo_name, drop_existing=False):
    if drop_existing:
        db_path = f'output/{repo_name}.db'
        if os.path.exists(db_path):
            os.remove(db_path)
    conn = connect_to_db(f'output/{repo_name}.db')                   
    insert_data(conn, repo_name)
    export_structure(conn)
    conn.close()

