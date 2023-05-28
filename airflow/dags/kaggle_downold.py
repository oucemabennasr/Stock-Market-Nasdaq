import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG(
    dag_id='download_stock_data',
    start_date=datetime(2023, 5, 28),
    schedule_interval=None  # Run the DAG only once
)

download_task = BashOperator(
    task_id='download_stock_data_task',
    bash_command="kaggle datasets download -d jacksoncrow/stock-market-dataset -p /path/to/data && \
                  unzip /path/to/data/stock-market-dataset.zip -d /path/to/data && \
                  mkdir /path/to/data/stock-market-dataset && \
                  mv /path/to/data/symbols_valid_meta.csv /path/to/data/stocks /path/to/data/etfs /path/to/data/stock-market-dataset || true",
    dag=dag
)

download_task
