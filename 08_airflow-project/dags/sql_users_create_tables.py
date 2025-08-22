from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="sql_users_create_tables",
    start_date=datetime(2025, 8, 22),
    schedule_interval="@once",
    catchup=False,
    tags=["sql", "auto-generated"]
) as dag:

    
    task_0 = PostgresOperator(
        task_id="task_0",
        postgres_conn_id="postgres_default",
        sql="""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER,
    city TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
    )
    
    
    