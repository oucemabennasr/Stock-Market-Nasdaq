#!/bin/bash
sudo apt-get update
sudo apt-get upgrade
#sudo pip3 install virtualenv
#virtualenv airflow_env
#source airflow_env/bin/activate
sudo apt-get install python3 python3-dev
sudo apt-get install openjdk-17-jdk
sudo apt install python3-pip
sudo pip3 install pandas
sudo pip3 install pyarrow
sudo pip3 install pyspark
sudo pip3 install -U scikit-learn
pip install xgboost
pip3 install apache-airflow[sentry,statsd]
pip3 install apache-airflow-providers-apache-spark
pip3 install apache-airflow-providers-cncf-kubernetes
mkdir airflow
cd airflow
airflow db init
#sed -i 's/load_examples = True/load_examples = False/' /home/cloud_user/airflow/airflow.cfg
#mkdir dags
#airflow users create --username oucema --firstname  oussama --lastname BenNasr --role Admin --email my@example.com --password mypass
#airflow scheduler
