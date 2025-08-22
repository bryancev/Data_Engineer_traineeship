# schedule: @daily
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import os

def load_users_from_csv():
    """Загружает данные пользователей из CSV файла в таблицу users используя INSERT"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    csv_path = "/opt/airflow/data/users.csv"
    
    # Проверяем существование файла
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV файл пользователей не найден: {csv_path}")
    
    print(f"Загрузка данных пользователей из {csv_path}")
    
    with open(csv_path, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_count = 0
        
        for row in reader:
            # Обрабатываем пустые значения
            user_id = int(row['id']) if row['id'] and row['id'].strip() else None
            name = row['name'].strip() if row['name'] else None
            age = int(row['age']) if row['age'] and row['age'].strip() else None
            city = row['city'].strip() if row['city'] else None
            
            # Валидация обязательных полей
            if not all([user_id, name]):
                print(f"Пропускаем некорректную строку: {row}")
                continue
            
            # Выполняем INSERT с обработкой конфликтов
            try:
                cur.execute("""
                    INSERT INTO users (id, name, age, city)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        age = EXCLUDED.age,
                        city = EXCLUDED.city
                """, (user_id, name, age, city))
                
                row_count += 1
                
            except Exception as e:
                print(f"Ошибка при вставке пользователя {user_id}: {e}")
                conn.rollback()
                raise
    
    conn.commit()
    print(f"Успешно загружено {row_count} записей пользователей в таблицу users")