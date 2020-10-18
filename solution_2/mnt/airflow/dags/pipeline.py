from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
import gzip
import pandas as pd
import psycopg2
import sys
import logging
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2020, 10, 15),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def parser(path):
    file_ = gzip.open(path, 'rb')
    for line in file_:
        yield eval(line)

def extract_keys(input_dict):
    """
    Extract pre-defined keys from the raw json data.

    Create the key, if the raw record does not contain the key.

    """

    d = {}
    for key in ['asin', 'title', 'price', 'brand']:
        if key in input_dict:
            d[key] = input_dict[key]
        else:
            d[key] = ''

    return d

def unzip_to_csv_sku(input_path, output_path):
    """
    Open .gzip -> extract keys -> store csv

    """

    i = 0
    records = {}

    for d in parser(input_path):
        records[i] = extract_keys(d)
        i += 1

    logging.info("Unzip complete: Saving to CSV")
    df = pd.DataFrame.from_dict(records, orient='index')
    df.to_csv(output_path, sep='|', index=False)

def unzip_to_csv_fact(input_path, output_path):
    """
    Open .gzip -> store csv

    """

    i = 0
    records = {}

    for d in parser(input_path):
        records[i] = d
        i += 1

    logging.info("Unzip complete: Saving to CSV")
    df = pd.DataFrame.from_dict(records, orient='index')
    df.to_csv(output_path, sep='|', index=False, )

def load_csv_to_table(file_path, table_name):
    """
    Loads a CSV using COPY command from SDTIN

    """

    try:
        logging.info("Connecting to the Database")
        conn = psycopg2.connect(dbname='airflow_db', host='postgres', port=5432, user='airflow', password='airflow')
        cursor = conn.cursor()
        logging.info("Success")

        # Read in file
        logging.info(f"Reading file: {file_path}")
        f = open(file_path, "r")

        # Truncate the table first
        cursor.execute("Truncate {} Cascade;".format(table_name))
        logging.info("Truncated {}".format(table_name))

        # Load table from the file with header
        cursor.copy_expert("copy {} from STDIN DELIMITER '|' CSV HEADER".format(table_name), f)
        cursor.execute("commit;")
        logging.info("Loaded data into {}".format(table_name))
        conn.close()
        logging.info("Connection closed.")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        sys.exit(1)


with DAG(
        dag_id="reviews_pipeline",
        schedule_interval="0 */8 * * *",
        default_args=default_args,
        catchup=False) as dag:

    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    unzip_data_store_as_csv_sku = PythonOperator(
        task_id='unzip_file_store_as_csv_sku',
        python_callable=unzip_to_csv_sku,
        op_kwargs={'input_path': '/usr/local/airflow/dags/files/meta_Musical_Instruments.json.gz',
                   'output_path': '/usr/local/airflow/dags/files/sku_data.csv'}
    )

    unzip_file_store_as_csv_fact = PythonOperator(
        task_id='unzip_file_store_as_csv_fact',
        python_callable=unzip_to_csv_fact,
        op_kwargs={'input_path': '/usr/local/airflow/dags/files/reviews_Musical_Instruments.json.gz',
                   'output_path': '/usr/local/airflow/dags/files/review_data.csv'}
    )

    create_staging_sku_table = PostgresOperator(
        task_id='create_staging_sku_table',
        postgres_conn_id='postgres_conn',
        sql="""
        drop table staging_sku;
        create table if not exists staging_sku (
            asin VARCHAR(256),
            title VARCHAR(10000),
            price DOUBLE PRECISION,
            brand VARCHAR(1000)); 
        """
    )

    create_staging_fact_table = PostgresOperator(
        task_id='create_staging_fact_table',
        postgres_conn_id='postgres_conn',
        sql="""
        drop table staging_review;
        create table if not exists staging_review (
            reviewerID VARCHAR(256),
            asin VARCHAR(256),
            reviewerName VARCHAR(256),
            helpful VARCHAR(256),
            reviewText VARCHAR(100000),
            overall DOUBLE PRECISION,
            summary VARCHAR(100000),
            unixReviewTime BIGINT,
            reviewTime VARCHAR(256));
        """
    )

    insert_to_staging_sku_table = PythonOperator(
        task_id='insert_to_staging_sku_table',
        python_callable=load_csv_to_table,
        op_kwargs={'file_path':'/usr/local/airflow/dags/files/sku_data.csv',
                   'table_name':'staging_sku'}
    )

    insert_to_staging_fact_table = PythonOperator(
        task_id='insert_to_staging_fact_table',
        python_callable=load_csv_to_table,
        op_kwargs={'file_path': '/usr/local/airflow/dags/files/review_data.csv',
                   'table_name': 'staging_review'}
    )

    create_and_load_sku_table = PostgresOperator(
        task_id='create_and_load_sku_table',
        postgres_conn_id='postgres_conn',
        sql='sql/create_and_load_sku.sql'
    )

    create_and_load_fact_table = PostgresOperator(
        task_id='create_and_load_fact_table',
        postgres_conn_id='postgres_conn',
        sql='sql/create_and_load_fact.sql'
    )


    start_pipeline >> unzip_data_store_as_csv_sku >> unzip_file_store_as_csv_fact \
    >> create_staging_sku_table >> create_staging_fact_table >> insert_to_staging_sku_table \
    >> insert_to_staging_fact_table >> create_and_load_sku_table >> create_and_load_fact_table