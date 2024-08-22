import os

import sqlite3
from sqlite3 import Error

# Function to create a database connection
def create_connection(db_file):

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Connected to test database.")
    except Error as e:
        print(e)
    return conn

# Function to create a table
def create_table(conn, create_table_sql):

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

# Main function to set up the database
def main_db():
    
    base_dir = os.path.dirname(os.path.realpath(__file__)) 
    database_path = os.path.join(base_dir, '../db/test_prod.db')
    
    db_dir = os.path.dirname(database_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    
    ##database = "../db/prod.db"  

    #Table creation query
    db_departments_table = """ CREATE TABLE IF NOT EXISTS departments (
                                        id INTEGER PRIMARY KEY,
                                        department STRING
                                    ); """

    db_jobs_table = """CREATE TABLE IF NOT EXISTS jobs (
                                id INTEGER PRIMARY KEY,
                                job STRING
                            );"""

    db_hired_employees_table = """CREATE TABLE IF NOT EXISTS hired_employees (
                                            id INTEGER PRIMARY KEY,
                                            name STRING,
                                            datetime STRING,
                                            department_id INTEGER,
                                            job_id INTEGER,
                                            FOREIGN KEY (department_id) REFERENCES departments (id),
                                            FOREIGN KEY (job_id) REFERENCES jobs (id)
                                        );"""
    db_quarters_table = """SELECT
                            d.department,
                            j.job,
                            SUM(CASE WHEN strftime('%m', e.datetime) IN ('01', '02', '03') THEN 1 ELSE 0 END) AS Q1,
                            SUM(CASE WHEN strftime('%m', e.datetime) IN ('04', '05', '06') THEN 1 ELSE 0 END) AS Q2,
                            SUM(CASE WHEN strftime('%m', e.datetime) IN ('07', '08', '09') THEN 1 ELSE 0 END) AS Q3,
                            SUM(CASE WHEN strftime('%m', e.datetime) IN ('10', '11', '12') THEN 1 ELSE 0 END) AS Q4
                        FROM hired_employees e
                        JOIN departments d ON e.department_id = d.id
                        JOIN jobs j ON e.job_id = j.id
                        WHERE strftime('%Y', e.datetime) = '2021'
                        GROUP BY d.department, j.job
                        ORDER BY d.department, j.job;"""

    #Database conn
    conn = create_connection(database_path)

    if conn is not None:
        create_table(conn, db_departments_table)
        create_table(conn, db_jobs_table)
        create_table(conn, db_hired_employees_table)

        print("Table created successfully.")
    else:
        print("Error! cant connect to the db.")

    # Close the connection
    conn.close()




if __name__ == '__main__':
    main_db()
