# schedule: @daily
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import os

def load_products_from_csv():
    """Загружает данные из CSV файла в таблицу products используя INSERT"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    csv_path = "/opt/airflow/data/products.csv"
    
    # Проверяем существование файла
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV файл не найден: {csv_path}")
    
    print(f"Загрузка данных из {csv_path}")
    
    with open(csv_path, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_count = 0
        
        for row in reader:
            # Обрабатываем пустые значения
            product_id = int(row['id']) if row['id'] else None
            name = row['name'] if row['name'] else None
            price = float(row['price']) if row['price'] else None
            category = row['category'] if row['category'] else None
            
            # Выполняем INSERT с обработкой конфликтов
            try:
                cur.execute("""
                    INSERT INTO products (id, name, price, category)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        price = EXCLUDED.price,
                        category = EXCLUDED.category
                """, (product_id, name, price, category))
                
                row_count += 1
                
            except Exception as e:
                print(f"Ошибка при вставке продукта {product_id}: {e}")
                conn.rollback()
                raise
    
    conn.commit()
    print(f"Успешно загружено {row_count} записей в таблицу products")