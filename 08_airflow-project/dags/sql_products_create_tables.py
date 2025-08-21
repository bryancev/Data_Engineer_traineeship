from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="sql_products_create_tables",
    start_date=datetime(2025, 8, 21),
    schedule_interval="@weekly",
    catchup=False,
    tags=["sql", "auto-generated"]
) as dag:

    
    task_0 = PostgresOperator(
        task_id="task_0",
        postgres_conn_id="postgres_default",
        sql="""
CREATE TABLE IF NOT EXISTS products (
    id INTEGER,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    category TEXT
);
"""
    )
    
    task_1 = PostgresOperator(
        task_id="task_1",
        postgres_conn_id="postgres_default",
        sql="""
CREATE TABLE IF NOT EXISTS products_transformed (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    category TEXT,
    price_category TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
    )
    
    
    
    task_0 >> task_1
    