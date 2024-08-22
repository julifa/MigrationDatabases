This API streamlines the process of transferring CSV data into a SQL database, offering a user-friendly RESTful interface for data management. With built-in backup and restore capabilities, you can safeguard your valuable information and easily restore it to any previous state.

It offers functionalities to create Avro backups of specific tables on demand.
You can restore a table from a backup file generated on a specific date (YYYY-MM-DD format).


The API includes two endpoints for generating reports:

-metrics/employees-by-quarter: This calculates the number of employees hired in each quarter (Q1-Q4) for a given year grouped by department and job title.

-metrics/departments-above-mean: This identifies departments that had a higher number of hires in 2021 compared to the average number of hires across all departments.