import logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
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

scripts_dir = '/home/oucemabennasr/Stock-Market-Nasdaq/src'
data_dir = '/home/oucemabennasr/Stock-Market-Nasdaq/data/raw_data'
parquet_file_path = '/home/oucemabennasr/Stock-Market-Nasdaq/data/processed_data/parquet_format'
output_dir = '/home/oucemabennasr/Stock-Market-Nasdaq/data/processed_data/parquet_format_avg_med'

process_raw_data_etfs = BashOperator(
    task_id='process_raw_data_etfs',
    bash_command=f'spark-submit {scripts_dir}/process_raw_data.py {data_dir} 20 etfs || true',
    trigger_rule='one_success',
    dag=dag,
)

feature_engineering_etfs = BashOperator(
    task_id='feature_engineering_etfs',
    bash_command=f'spark-submit {scripts_dir}/feature_engineering.py {parquet_file_path} etfs {output_dir} || true',
    trigger_rule='one_success',
    dag=dag,
)

ml_training_etfs = BashOperator(
    task_id='ml_training_etfs',
    bash_command=f'python {scripts_dir}/ml_training.py etfs || true',
    trigger_rule='all_success',
    dag=dag,
)

process_raw_data_stocks = BashOperator(
    task_id='process_raw_data_stocks',
    bash_command=f'spark-submit {scripts_dir}/process_raw_data.py {data_dir} 20 stocks || true',
    trigger_rule='one_success',
    dag=dag,
)

feature_engineering_stocks = BashOperator(
    task_id='feature_engineering_stocks'',
    bash_command=f'spark-submit {scripts_dir}/feature_engineering.py {parquet_file_path} stocks {output_dir} || true',
    trigger_rule='one_success',
    dag=dag,
)

ml_training_etfs = BashOperator(
    task_id='ml_training_stocks'',
    bash_command=f'{scripts_dir}/ml_training.py stocks || true',
    trigger_rule='all_success',
    dag=dag,
)


process_raw_data_etfs >> feature_engineering_etfs >> ml_training_etfs
process_raw_data_stocks >> feature_engineering_stocks >> ml_training_stocks
