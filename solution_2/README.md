## Solution 2:

* Spin up docker-containers using docker-compose up -d
* This will configure airflow and a postgresdb.

Configure the postgres connection in Airflow:
* name: postgres_conn
* host: postgres
* schema: airflow_db
* login: airflow
* pass: airflow
