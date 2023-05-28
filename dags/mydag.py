import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
#from airflow.task.log_task_handler import FileTaskHandler

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

# Define the log file path
#log_file_path = '/home/cloud_user/airflow/Stock-Market-Nasdaq/logfile.log'

# Create a FileTaskHandler to handle logging to the file
#file_handler = FileTaskHandler(log_file_path)
#file_handler.setLevel(logging.INFO)

process_raw_data_etfs = SparkSubmitOperator(
    task_id='process_raw_data_etfs',
    application='/home/cloud_user/airflow/Stock-Market-Nasdaq/scripts/process_raw_data.py',
    application_args=['/home/cloud_user/airflow/Stock-Market-Nasdaq/data', '200', 'etfs'],
    dag=dag,
)

feature_engineering_etfs = SparkSubmitOperator(
    task_id='feature_engineering_etfs',
    application='/home/cloud_user/airflow/Stock-Market-Nasdaq/scripts/feature_engineering.py',
    conf={'spark.master': 'spark://spark-master:7077'},
    application_args=['/home/cloud_user/airflow/Stock-Market-Nasdaq/data/parquet_file', 'etfs'],
    dag=dag,
)

ml_training_etfs = BashOperator(
    task_id='ml_training_etfs',
    bash_command='/home/cloud_user/airflow/Stock-Market-Nasdaq/scripts/ml_training',
    dag=dag,
)

process_raw_data_stocks = SparkSubmitOperator(
    task_id='process_raw_data_stocks',
    application='/home/cloud_user/airflow/Stock-Market-Nasdaq/scripts/process_raw_data.py',
    application_args=['/home/cloud_user/airflow/Stock-Market-Nasdaq/data', '200', 'stocks'],
    dag=dag,
)

feature_engineering_stocks = SparkSubmitOperator(
    task_id='feature_engineering_stocks',
    application='/path/to/feature_engineering.py',
    conf={'spark.master': 'spark://spark-master:7077'},
    application_args=['/home/cloud_user/airflow/Stock-Market-Nasdaq/data/parquet_file', 'stocks'],
    dag=dag,
)

#ml_training_stocks = PythonOperator(
#    task_id='ml_training_stocks',
#    python_callable=ml_training,
#    dag=dag,
#)

# Assign the FileTaskHandler to the tasks to enable logging to the file
#for task in [process_raw_data_etfs, process_raw_data_stocks, feature_engineering_etfs, feature_engineering_stocks,
#             ml_training_etfs, ml_training_stocks]:
#    task.handlers = [file_handler]
process_raw_data_etfs.set_downstream(feature_engineering_etfs)
process_raw_data_stocks.set_downstream(feature_engineering_stocks)

feature_engineering_etfs.set_downstream(ml_training_etfs)
feature_engineering_stocks.set_downstream(ml_training_etfs)

ml_training_etfs
#[process_raw_data_etfs, process_raw_data_stocks] >> [feature_engineering_etfs, feature_engineering_stocks]
#[feature_engineering_etfs, feature_engineering_stocks] >> [ml_training_etfs]
#[ml_training_etfs]
