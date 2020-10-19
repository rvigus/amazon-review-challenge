from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
import gzip
import pandas as pd
import psycopg2
import sys
import os
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

def unzip_to_csv_sku(input_path, output_path, name):
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
    df['link_category'] = name.split('_', 1)[1] # split on first _ take second element in list.
    df.to_csv(output_path, sep='|', index=False)

def iterate_over_sku_files(folder_path):
    """
    Process all SKU data in the folder

    """

    files = os.listdir(folder_path)
    gzips = [f for f in files if 'meta' in f]

    for g in gzips:
        input_path = os.path.join(folder_path, g)
        name = g.split('.json')[0]
        output_path = os.path.join(folder_path, f"{name}.csv")

        logging.info(f"Extracting from: {input_path} to {output_path}")
        unzip_to_csv_sku(input_path=input_path, output_path=output_path, name=name)

def unzip_to_csv_fact(input_path, output_path, name):
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
    df['link_category'] = name.split('_', 1)[1]
    df = df.drop_duplicates(subset=['reviewerID', 'asin', 'unixReviewTime'])
    df.iloc[:500000, :].to_csv(output_path, sep='|', index=False)

def iterate_over_fact_files(folder_path):
    """
    Process all FACT data in the folder

    """

    files = os.listdir(folder_path)
    gzips = [f for f in files if 'reviews' in f]

    for g in gzips:
        input_path = os.path.join(folder_path, g)
        name = g.split('.json')[0]
        output_path = os.path.join(folder_path, f"{name}.csv")

        logging.info(f"Extracting from: {input_path} to {output_path}")
        unzip_to_csv_fact(input_path=input_path, output_path=output_path, name=name)

def load_csv_to_table(file_path, table_name, iter):
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

        # Truncate the table first if this is the first csv to be written
        if iter == 0:
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

def iterate_csv_load(folder_path, type, table_name):
    """
    Load all csv's to postgres staging

    """

    files = os.listdir(folder_path)
    csvs = [f for f in files if type in f]
    csvs = [f for f in csvs if '.csv' in f]
    logging.info("Files found: {}".format(csvs))

    for i, c in enumerate(csvs):
        input_path = os.path.join(folder_path, c)
        load_csv_to_table(file_path=input_path, table_name=table_name, iter=i)


with DAG(
        dag_id="reviews_pipeline",
        schedule_interval="0 */8 * * *",
        default_args=default_args,
        catchup=False) as dag:

    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    download_data_sets = BashOperator(
        task_id='download_data_sets',
        bash_command='/bash/download.sh'
    )

    extract_sku_data_to_csv = PythonOperator(
        task_id='extract_sku_data_to_csv',
        python_callable=iterate_over_sku_files,
        op_kwargs={'folder_path':'/usr/local/airflow/dags/files'}
    )

    extract_fact_data_to_csv = PythonOperator(
        task_id='extract_fact_data_to_csv',
        python_callable=iterate_over_fact_files,
        op_kwargs={'folder_path': '/usr/local/airflow/dags/files'}
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
            brand VARCHAR(1000),
            link_category VARCHAR(256)); 
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
            reviewTime VARCHAR(256),
            link_category VARCHAR(256));
        """
    )

    insert_to_staging_sku_table = PythonOperator(
        task_id='insert_to_staging_sku_table',
        python_callable=iterate_csv_load,
        op_kwargs={'folder_path':'/usr/local/airflow/dags/files',
                   'type':'meta',
                   'table_name': 'staging_sku'}
    )

    insert_to_staging_fact_table = PythonOperator(
        task_id='insert_to_staging_fact_table',
        python_callable=iterate_csv_load,
        op_kwargs={'folder_path':'/usr/local/airflow/dags/files',
                   'type':'review',
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

    clean_up_files = BashOperator(
        task_id='clean_up_files',
        bash_command='bash/cleanup.sh',
        trigger_rule='all_done'
    )


    start_pipeline >> download_data_sets >> extract_sku_data_to_csv \
    >> extract_fact_data_to_csv >> create_staging_sku_table >> create_staging_fact_table \
    >> insert_to_staging_sku_table >> insert_to_staging_fact_table >> create_and_load_sku_table >> create_and_load_fact_table \
    >> clean_up_files