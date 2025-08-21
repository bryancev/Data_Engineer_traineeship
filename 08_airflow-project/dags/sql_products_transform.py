from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="sql_products_transform",
    start_date=datetime(2025, 8, 21),
    schedule_interval="@daily",
    catchup=False,
    tags=["sql", "auto-generated"]
) as dag:

    
    task_0 = PostgresOperator(
        task_id="task_0",
        postgres_conn_id="postgres_default",
        sql="""
UPDATE products 
SET category = 'Unknown' 
WHERE category IS NULL OR category = '';
"""
    )
    
    task_1 = PostgresOperator(
        task_id="task_1",
        postgres_conn_id="postgres_default",
        sql="""
INSERT INTO products_transformed (id, name, price, category, price_category, created_at)
SELECT DISTINCT ON (id)
    id,
    name,
    price,
    COALESCE(category, 'Unknown'),
    CASE 
        WHEN price < 1000 THEN 'Budget'
        WHEN price BETWEEN 1000 AND 5000 THEN 'Medium'
        ELSE 'Premium'
    END,
    CURRENT_TIMESTAMP
FROM products
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    category = EXCLUDED.category,
    price_category = EXCLUDED.price_category,
    created_at = CURRENT_TIMESTAMP;
"""
    )
    
    
    
    task_0 >> task_1
    