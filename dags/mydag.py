  GNU nano 4.8                                                                                                                                                                                                                                                                                                    airflow/dags/mydag.py                                                                                                                                                                                                                                                                                                              import logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
default_args = {
    'owner': 'airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    dag_id='stock_market_dag',
    default_args=default_args,
    start_date = airflow.utils.dates.days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    schedule_interval='@once',
    description='DAG for stock market data processing and machine learning training',
)

scripts_dir = '/home/cloud_user/Stock-Market-Nasdaq/scripts'
data_dir = '/home/cloud_user/Stock-Market-Nasdaq/data'
parquet_file_path = f'{data_dir}/parquet_file'
output_dir = f'{data_dir}/feacher_ing'

process_raw_data_etfs = SparkSubmitOperator(
    task_id='process_raw_data_etfs',
    conn_id= 'spark_local',
    application=f'{scripts_dir}/process_raw_data.py',
    application_args=[data_dir, '200', 'etfs'],
    dag=dag,
)

feature_engineering_etfs = SparkSubmitOperator(
    task_id='feature_engineering_etfs',
    conn_id= 'spark_local',
    application=f'{scripts_dir}/feature_engineering.py',
    application_args=[parquet_file_path, 'etfs',output_dir],
    dag=dag,
)

ml_training_etfs = BashOperator(
    task_id='ml_training_etfs',
    bash_command=f'{scripts_dir}/ml_training',
    dag=dag,
)

process_raw_data_stocks = SparkSubmitOperator(
    task_id='process_raw_data_stocks',
    conn_id= 'spark_local',
    application=f'{scripts_dir}/process_raw_data.py',
    application_args=[data_dir, '200', 'stocks'],
    dag=dag,
)

feature_engineering_stocks = SparkSubmitOperator(
    task_id='feature_engineering_stocks',
    conn_id= 'spark_local',
    application='/path/to/feature_engineering.py',
    application_args=[parquet_file_path, 'stocks', output_dir],
    dag=dag,
)

process_raw_data_etfs >> feature_engineering_etfs >> ml_training_etfs
process_raw_data_stocks >> feature_engineering_stocks
#if __name__ == "__main__":
#    dag.cli()
