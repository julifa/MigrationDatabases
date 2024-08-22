import os
import fastavro
from sqlite3 import connect
import pandas as pd

def backup_table_to_avro(table_name, db_path, avro_file_path):
    base_dir = os.path.abspath(os.path.dirname(__file__))  # Absolute path to the api folder
    database_path = os.path.join(base_dir, '../db/test_prod.db')
    conn = connect(database_path)
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()

    # Fetch the column names from the cursor
    column_names = [description[0] for description in cursor.description]

    # You might need to adjust the types according to the actual data types of your columns
    schema = {
        'doc': f'Backup of the {table_name} table',
        'name': 'AutoGenerated',
        'type': 'record',
        'fields': [{'name': col_name, 'type': ['null', 'string']} for col_name in column_names],
    }
    # Write the records to an AVRO file
    with open(avro_file_path, 'wb') as out:
        fastavro.writer(out, schema, records)

    # Close the database connection
    conn.close()

def restore_table_from_avro(table_name, db_path, avro_file_path):

    base_dir = os.path.abspath(os.path.dirname(__file__))  # Absolute path to the api folder
    database_path = os.path.join(base_dir, '../db/test_prod.db')  # Corrected path to the db folder
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Clear the table before restoring
    cursor.execute(f'DELETE FROM {table_name}')
    conn.commit()

    with open(avro_file_path, 'rb') as avro_file:
        reader = fastavro.reader(avro_file)
        records = [record for record in reader]

    # Convert the records to a DataFrame and write to the SQL table
    df = pd.DataFrame(records)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
