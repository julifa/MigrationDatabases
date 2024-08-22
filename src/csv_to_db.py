import pandas as pd
import sqlite3
import os

from sqlite3 import Error

def read_csv_to_df(file_path: str, column_names: list) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.
    """
    return pd.read_csv(file_path, header=None, names=column_names)

def insert_df_to_db(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection, if_exists: str = 'append'):
    """
    Insert DataFrame into the database table.
    """
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    print(f"Inserted {len(df)} rows into {table_name} table successfully.")

def create_connection(db_file):

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Connected to test database.")
    except Error as e:
        print(e)
    return conn

def main_csv_to_db(hired_employees_file_path, departments_file_path, jobs_file_path, database_path):

    conn = create_connection(database_path)

    hired_employees_df = read_csv_to_df(hired_employees_file_path, hired_employees_columns)
    departments_df = read_csv_to_df(departments_file_path, departments_columns)
    jobs_df = read_csv_to_df(jobs_file_path, jobs_columns)

    insert_df_to_db(hired_employees_df, 'hired_employees', conn)
    insert_df_to_db(departments_df, 'departments', conn)
    insert_df_to_db(jobs_df, 'jobs', conn)

    conn.commit()
    conn.close()


hired_employees_columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
departments_columns = ['id', 'department']
jobs_columns = ['id', 'job']

#Using OS library to get the current path
base_dir = os.path.dirname(os.path.realpath(__file__))

database_path = os.path.join(base_dir, '../db/test_prod.db')

hired_employees_file_path = os.path.join(base_dir, '../raw_csv/hired_employees.csv')
departments_file_path = os.path.join(base_dir, '../raw_csv/departments.csv')
jobs_file_path = os.path.join(base_dir, '../raw_csv/jobs.csv')

#Creating database directory if it doesn't exist
db_dir = os.path.dirname(database_path)
if not os.path.exists(db_dir):
    os.makedirs(db_dir)

if __name__ == '__main__':
    main_csv_to_db(hired_employees_file_path, departments_file_path, jobs_file_path, database_path)
