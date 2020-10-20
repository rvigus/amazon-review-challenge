# Solution 2:

## Initial setup

* Spin up docker-containers using docker-compose up -d
* This will configure airflow and a postgresdb.

You will need to configure the postgres connection in Airflow (go to admin -> connections):
* name: postgres_conn
* host: postgres
* schema: airflow_db
* login: airflow
* pass: airflow

Launch airflow @ localhost:8080

## Overview
This pipeline will fetch 4 review categories and their metadata from https://jmcauley.ucsd.edu/data/amazon/links.html

We process these files in python, copy them to staging tables and finally transform to fact and dimension tables.

This pipeline is scheduled to run every 8 hours, and will initiate a FULL LOAD. The assumption made in this case is that
data is updated every 8 hours on the website.

The resulting tables (fact_reviews, dim_sku) should be queryable via a tool of your choice, for me I was using dbeaver.