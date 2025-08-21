from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv

def load_csv():
    """Загружает данные из CSV в таблицу"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    with open("/opt/airflow/data/products.csv", "r", encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Пропускаем заголовок
        
        for row in reader:
            # Преобразуем пустые строки в None
            processed_row = [None if cell == '' else cell for cell in row]
            cur.execute(
                "INSERT INTO products VALUES (%s, %s, %s, %s)",
                processed_row
            )
    conn.commit()
    print(f"Data loaded successfully into products")

with DAG(
    dag_id="csv_products_load",
    start_date=datetime(2025, 8, 21),
    schedule_interval="@daily",
    catchup=False,
    tags=["csv", "auto-generated"]
) as dag:

    
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_csv
    )
    
    load_data