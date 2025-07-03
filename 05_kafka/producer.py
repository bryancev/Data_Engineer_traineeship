import psycopg2
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# 1. НАСТРОЙКИ ПОДКЛЮЧЕНИЯ
KAFKA_SERVERS = 'localhost:9092'
DB_CONFIG = {
    "dbname": "test_db",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

# 2. ИНИЦИАЛИЗАЦИЯ ПРОДЮСЕРА KAFKA
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 3. ПОДКЛЮЧЕНИЕ К POSTGRESQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# 4. СОЗДАНИЕ ТАБЛИЦЫ USER_LOGINS
cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
)
""")
conn.commit()

# 5. ГЕНЕРАЦИЯ И СОХРАНЕНИЕ ТЕСТОВЫХ ДАННЫХ
print("Генерация тестовых данных...")
users = ["alice", "bob", "carol", "dave"]
events = ["login", "logout", "view", "purchase"]

for i in range(1, 11):  # Генерируем 10 тестовых событий
    event_data = {
        "user": random.choice(users),
        "event": random.choice(events),
        "timestamp": datetime.now().timestamp()
    }
    
    # Сохраняем в PostgreSQL
    cursor.execute(
        "INSERT INTO user_logins (username, event_type, event_time) VALUES (%s, %s, to_timestamp(%s))",
        (event_data["user"], event_data["event"], event_data["timestamp"])
    )
    conn.commit()
    print(f"Сохранено в БД: {event_data}")
    time.sleep(0.5)

# 6. ОБРАБОТКА ДАННЫХ ИЗ POSTGRESQL
print("\nОбработка данных из PostgreSQL...")
cursor.execute("""
    SELECT
        id, 
        username, 
        event_type, 
        extract(epoch FROM event_time)
    FROM user_logins
    WHERE sent_to_kafka = FALSE
""")
rows = cursor.fetchall()

# Обрабатываем каждую запись
for row in rows:
    data = {
        "id": row[0],
        "user": row[1],
        "event": row[2],
        "timestamp": float(row[3])  # явное преобразование Decimal в float
    }
    
    # Отправляем в Kafka
    producer.send("user_events", value=data)
    print(f"Отправлено в Kafka: {data}")
    
    # Помечаем как отправленное по ID записи
    cursor.execute(
        "UPDATE user_logins SET sent_to_kafka = TRUE WHERE id = %s",
        (data["id"],)
    )
    conn.commit()
    time.sleep(0.3)

# 7. ЗАВЕРШЕНИЕ РАБОТЫ
print("\nЗавершение работы...")
cursor.close()
conn.close()
producer.close()
print("Все операции завершены")