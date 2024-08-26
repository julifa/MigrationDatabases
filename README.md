This API streamlines the process of transferring CSV data into a SQL database, offering a user-friendly RESTful interface for data management. With built-in backup and restore capabilities, you can safeguard your valuable information and easily restore it to any previous state.

Also includes two endpoints for generating reports:

-metrics/employees-by-quarter: This calculates the number of employees hired in each quarter (Q1-Q4) for a given year grouped by department and job title.

-metrics/departments-above-mean: This identifies departments that had a higher number of hires in 2021 compared to the average number of hires across all departments.



Guide
-----


git clone https://github.com/julifa/MigrationDatabases.git

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt

python src/databases.py
python src/csv_to_db.py

python api/main.py


Endpoints

1. GET /metrics/employees-by-quarter - The response will be the table with the results, in alphabetical order in both the jobs and departments column
2. GET /metrics/departments-above-mean - The response will be the departments that hired more than the mean of employees hired by departments, its sorted in a descending way
