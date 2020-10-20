from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
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

def unzip_to_csv(input_path, output_path):
    """
    Open .gzip write to json

    """
    i = 0
    records = {}

    for d in parser(input_path):
        records[i] = d
        i += 1

    # records = {0: records[0], 1: records[1], 2: records[2], 3: records[3], 4: records[4]}

    df = pd.DataFrame.from_dict(records, orient='index')
    df['reviewText'] = df['reviewText'].apply(lambda x: x.replace('|', ''))
    df.to_csv(output_path, sep='|', index=False)


with DAG(
        dag_id="create_fact_review_table",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False) as dag:


    # Step 1: Unzip and store as csv
    unzip_file_store_as_csv = PythonOperator(
        task_id='unzip_file_store_as_csv',
        python_callable=unzip_to_csv,
        op_kwargs={'input_path': '/usr/local/airflow/dags/files/reviews_Musical_Instruments.json.gz',
                   'output_path': '/usr/local/airflow/dags/files/review_data.csv'}
    )

    # Step 2: Move json file to hdfs storage
    move_to_hdfs = BashOperator(
        task_id="move_to_hdfs",
        bash_command="""
            hdfs dfs -mkdir -p /fact_review && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/review_data.csv /fact_review
            """
    )

    # Step 3: Create a hive table on our sku_data
    creating_fact_table = HiveOperator(
        task_id="creating_fact_review_table",
        hive_cli_conn_id="hive_conn",
        hql="""
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

    processing_fact_data = SparkSubmitOperator(
        task_id="processing_fact_data",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/fact_review_processing.py",
        verbose=False
    )


    unzip_file_store_as_csv >> move_to_hdfs >> creating_fact_table >> processing_fact_data
