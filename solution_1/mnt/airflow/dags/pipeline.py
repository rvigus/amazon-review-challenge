from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
import gzip
import pandas as pd

default_args = {
    "owner" : "airflow", # who can run
    "start_date" : datetime(2020, 10, 15), # when to run
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def parser(path):
    file_ = gzip.open(path, 'rb')
    for line in file_:
        yield eval(line)

def extract_keys(input_dict):
    """
    Extract pre-defined keys from the raw data.

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

    # records = {0: records[0], 1: records[1], 2: records[2], 3: records[3], 4: records[4]}
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

    # records = {0: records[0], 1: records[1], 2: records[2], 3: records[3], 4: records[4]}
    df = pd.DataFrame.from_dict(records, orient='index')
    df.to_csv(output_path, sep='|', index=False)


with DAG(
        dag_id="reviews_pipeline",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False) as dag:

    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    # Stage 1: Extract zips

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

    # Stage 2: Transfer to  HDFS

    move_to_hdfs_sku = BashOperator(
        task_id="move_to_hdfs_sku",
        bash_command="""
            hdfs dfs -mkdir -p /dim_sku && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/sku_data.csv /dim_sku
            """
    )

    move_to_hdfs_fact = BashOperator(
        task_id="move_to_hdfs_fact",
        bash_command="""
            hdfs dfs -mkdir -p /fact_review && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/review_data.csv /fact_review
            """
    )

    # Stage 3: Create Hive tables

    creating_sku_table = HiveOperator(
        task_id="creating_sku_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            DROP TABLE IF EXISTS dim_sku;
            CREATE EXTERNAL TABLE IF NOT EXISTS dim_sku(
                asin STRING,
                title STRING,
                price DOUBLE,
                brand STRING
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '|'
            STORED AS TEXTFILE
        """
    )

    creating_fact_table = HiveOperator(
        task_id="creating_fact_review_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            DROP TABLE IF EXISTS fact_review;
            CREATE EXTERNAL TABLE IF NOT EXISTS fact_review(
                reviewerID STRING,
                asin STRING,
                reviewerName STRING,
                helpful STRING,
                reviewText STRING,
                overall INT,
                summary STRING,
                unixReviewTime BIGINT,
                reviewTime STRING
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '|'
            STORED AS TEXTFILE
        """
    )

    # Stage 4: Trigger spark job to 1) Remove duplicates 2) Write data to table

    processing_sku_data = SparkSubmitOperator(
        task_id="processing_sku_data",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/dim_sku_processing.py",
        verbose=False
    )

    processing_fact_data = SparkSubmitOperator(
        task_id="processing_fact_data",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/fact_review_processing.py",
        verbose=False
    )


    # Set dependencies
    start_pipeline >> [unzip_data_store_as_csv_sku, unzip_file_store_as_csv_fact]
    unzip_data_store_as_csv_sku >> move_to_hdfs_sku >> creating_sku_table >> processing_sku_data
    unzip_file_store_as_csv_fact >> move_to_hdfs_fact >> creating_fact_table >> processing_fact_data
