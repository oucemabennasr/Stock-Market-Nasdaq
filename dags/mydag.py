import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_market_dag',
    default_args=default_args,
    description='DAG for stock market data processing and machine learning training',
)

scripts_dir = '/home/cloud_user/airflow/Stock-Market-Nasdaq/scripts'
data_dir = '/home/cloud_user/airflow/Stock-Market-Nasdaq/data'
parquet_file_path = f'{data_dir}/parquet_file'

process_raw_data_etfs = SparkSubmitOperator(
    task_id='process_raw_data_etfs',
    application=f'{scripts_dir}/process_raw_data.py',
    application_args=[data_dir, '200', 'etfs'],
    dag=dag,
)

feature_engineering_etfs = SparkSubmitOperator(
    task_id='feature_engineering_etfs',
    application=f'{scripts_dir}/feature_engineering.py',
    conf={'spark.master': 'spark://spark-master:7077'},
    application_args=[parquet_file_path, 'etfs'],
    dag=dag,
)

ml_training_etfs = BashOperator(
    task_id='ml_training_etfs',
    bash_command=f'{scripts_dir}/ml_training',
    dag=dag,
)

process_raw_data_stocks = SparkSubmitOperator(
    task_id='process_raw_data_stocks',
    application=f'{scripts_dir}/process_raw_data.py',
    application_args=[data_dir, '200', 'stocks'],
    dag=dag,
)

feature_engineering_stocks = SparkSubmitOperator(
    task_id='feature_engineering_stocks',
    application='/path/to/feature_engineering.py',
    conf={'spark.master': 'spark://spark-master:7077'},
    application_args=[parquet_file_path, 'stocks'],
    dag=dag,
)

process_raw_data_etfs >> feature_engineering_etfs >> ml_training_etfs
process_raw_data_stocks >> feature_engineering_stocks
