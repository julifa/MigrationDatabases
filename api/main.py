import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from flask import Flask, request, jsonify, send_from_directory
from flask_restful import Resource, Api
from src.models.schemas import EmployeeModel, DepartmentModel, JobModel
from src.backup_utils import backup_table_to_avro, restore_table_from_avro
import pandas as pd
import sqlite3
from sqlite3 import Error
import pydantic
from pydantic import ValidationError

from datetime import datetime

import fastavro

app = Flask(__name__)
api = Api(app)

db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASS')

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

# Establish a connection to the database
def create_connection():
    conn = None
    base_dir = os.path.abspath(os.path.dirname(__file__))  # Absolute path to the api folder
    database_path = os.path.join(base_dir, '../db/test_prod.db')  #Path to the db folder
    try:
        conn = sqlite3.connect(database_path)
        return conn
    except Error as e:
        print(e)
    return conn

# Define a resource for the hired_employees
class HiredEmployees(Resource):
    def get(self):
        conn = create_connection()
        df = pd.read_sql_query("SELECT * FROM hired_employees", conn)
        conn.close()
        return jsonify(df.to_dict(orient='records'))

    def post(self):
        conn = create_connection()

        data = request.get_json()

        if isinstance(data, dict):
            data = [data]  # Now `data` is a list with one dict

        # Ensure that data is a list of records
        if not isinstance(data, list):
            return {'message': 'Input data should be a list of records'}, 400

        # Check if the batch size is within the limit
        if len(data) > 1000:
            return {'message': 'Batch size exceeds the limit of 1000 rows'}, 400

        try:
            # Start a transaction
            with conn:
                # Validate and insert each record in the batch
                for item in data:
                    validated_item = EmployeeModel(**item)
                    df = pd.DataFrame([validated_item.dict()])
                    df.to_sql('hired_employees', conn, if_exists='append', index=False)

            return {'message': 'Batch data inserted successfully'}, 201

        except ValidationError as e:
            return {'message': 'Validation error', 'errors': e.errors()}, 400
        except Error as e:
            return {'message': 'Database error', 'errors': str(e)}, 500
        finally:
            conn.close()


class Departments(Resource):
    def get(self):
        conn = create_connection()
        df = pd.read_sql_query("SELECT * FROM departments", conn)
        conn.close()
        return jsonify(df.to_dict(orient='records'))

    def post(self):
        conn = create_connection()
        data = request.get_json()

        # Wrap single record into a list if it's a dictionary
        if isinstance(data, dict):
            data = [data]

        # Ensure the batch size limit
        if len(data) > 1000:
            return {'message': 'Batch size exceeds the limit of 1000 rows'}, 400

        try:
            validated_data = [DepartmentModel(**item) for item in data]
            insert_data = [item.dict() for item in validated_data]
            
            df = pd.DataFrame(insert_data)
            df.to_sql('departments', conn, if_exists='append', index=False)
            conn.commit()
            return {'message': 'Data inserted successfully'}, 201

        except ValidationError as e:
            return {'message': 'Validation error', 'errors': e.errors()}, 400
        finally:
            conn.close()

class Jobs(Resource):
    def get(self):
        conn = create_connection()
        df = pd.read_sql_query("SELECT * FROM jobs", conn)
        conn.close()
        return jsonify(df.to_dict(orient='records'))

    def post(self):
        conn = create_connection()
        data = request.get_json()

        # Wrap single record into a list if it's a dictionary
        if isinstance(data, dict):
            data = [data]

        # Ensure the batch size limit
        if len(data) > 1000:
            return {'message': 'Batch size exceeds the limit of 1000 rows'}, 400

        try:
            validated_data = [JobModel(**item) for item in data]
            insert_data = [item.dict() for item in validated_data]
            df = pd.DataFrame(insert_data)
            df.to_sql('jobs', conn, if_exists='append', index=False)
            conn.commit()
            return {'message': 'Data inserted successfully'}, 201

        except ValidationError as e:
            return {'message': 'Validation error', 'errors': e.errors()}, 400
        finally:
            conn.close()

class Backup(Resource):
    def get(self, table_name):
        # Define the backup file path with the current date
        date_str = datetime.now().strftime("%Y-%m-%d")
        base_dir = os.path.abspath(os.path.dirname(__file__))
        backup_dir = os.path.join(base_dir, '../backup')
        backup_file_path = os.path.join(backup_dir, f'{table_name}_{date_str}.avro')
        
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        try:
            # Ensure the database path is also absolute
            db_path = os.path.join(base_dir, '../db/test_prod.db')
            backup_table_to_avro(table_name, db_path, backup_file_path)
            
            # Send the file
            return send_from_directory(directory=os.path.dirname(backup_file_path),filename=os.path.basename(backup_file_path),as_attachment=True)
        except Exception as e:
            return {'message': 'Backup failed', 'errors': str(e)}, 500

class Restore(Resource):
    def post(self, table_name):
        # Retrieve the date from the request to find the backup file
        data = request.get_json()
        backup_date = data.get('date')  # Expecting a date in 'YYYY-MM-DD' format
        if not backup_date:
            return {'message': 'Date is required for restore operation'}, 400
        
        base_dir = os.path.abspath(os.path.dirname(__file__))
        backup_file_path = os.path.join(base_dir, f'../backup/{table_name}_{backup_date}.avro')
        
        try:
            # Ensure the database path is also absolute
            db_path = os.path.join(base_dir, '../db/test_prod.db')
            restore_table_from_avro(table_name, db_path, backup_file_path)
            
            return {'message': f'Table {table_name} restored successfully'}, 200
        except FileNotFoundError:
            return {'message': 'Backup file not found'}, 404
        except Exception as e:
            return {'message': 'Restore failed', 'errors': str(e)}, 500


class EmployeeStatsByQuarter(Resource):
    def get(self):
        conn = create_connection()
        query = """
        SELECT
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
        ORDER BY d.department, j.job;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return jsonify(df.to_dict(orient='records'))


class DepartmentsAboveMean(Resource):
    def get(self):
        conn = create_connection()
        query = """
        WITH DepartmentHires AS (
            SELECT
                d.id,
                d.department,
                COUNT(e.id) as hired
            FROM departments d
            LEFT JOIN hired_employees e ON d.id = e.department_id
                AND strftime('%Y', e.datetime) = '2021'
            GROUP BY d.id
        ),
        AverageHires AS (
            SELECT AVG(hired) as average_hired FROM DepartmentHires
        )
        SELECT
            dh.id,
            dh.department,
            dh.hired
        FROM DepartmentHires dh, AverageHires
        WHERE dh.hired > AverageHires.average_hired
        ORDER BY dh.hired DESC;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return jsonify(df.to_dict(orient='records'))


# Add the resource to the API
api.add_resource(HiredEmployees, '/hired_employees')
api.add_resource(Departments, '/departments')
api.add_resource(Jobs, '/jobs')
api.add_resource(Backup, '/backup/<string:table_name>')
api.add_resource(Restore, '/restore/<string:table_name>')
api.add_resource(EmployeeStatsByQuarter, '/metrics/employees-by-quarter')
api.add_resource(DepartmentsAboveMean, '/metrics/departments-above-mean')

if __name__ == '__main__':
    app.run(debug=True)
