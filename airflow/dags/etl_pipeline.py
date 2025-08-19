from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/etl")
from fetch_data import fetch_products
from transform_data import transform_products
from load_to_db import load_to_db

def fetch_task():
    fetch_products()

def transform_task():
    transform_products()

def load_task():
    load_to_db()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 18),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
)

fetch = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load_to_db',
    python_callable=load_task,
    dag=dag,
)

fetch >> transform >> load
