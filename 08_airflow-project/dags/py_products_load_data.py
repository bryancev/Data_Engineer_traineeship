from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append("/opt/airflow/scripts/python")

from products_load_data import load_products_from_csv

with DAG(
    dag_id="py_products_load_data",
    start_date=datetime(2025, 8, 22),
    schedule_interval="@daily",
    catchup=False,
    tags=["python", "auto-generated"]
) as dag:

    
    load_products_from_csv_task = PythonOperator(
        task_id="load_products_from_csv",
        python_callable=load_products_from_csv
    )
    