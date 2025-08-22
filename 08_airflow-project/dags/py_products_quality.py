from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append("/opt/airflow/scripts/python")

from products_quality import monitor_data_quality

with DAG(
    dag_id="py_products_quality",
    start_date=datetime(2025, 8, 22),
    schedule_interval="@daily",
    catchup=False,
    tags=["python", "auto-generated"]
) as dag:

    
    monitor_data_quality_task = PythonOperator(
        task_id="monitor_data_quality",
        python_callable=monitor_data_quality
    )
    