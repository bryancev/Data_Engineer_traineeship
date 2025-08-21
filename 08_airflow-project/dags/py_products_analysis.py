from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append("/opt/airflow/scripts/python")

from products_analysis import generate_products_report

with DAG(
    dag_id="py_products_analysis",
    start_date=datetime(2025, 8, 21),
    schedule_interval="@daily",
    catchup=False,
    tags=["python", "auto-generated"]
) as dag:

    
    generate_products_report_task = PythonOperator(
        task_id="generate_products_report",
        python_callable=generate_products_report
    )
    