from kafka import KafkaConsumer
import json
import clickhouse_connect

# 1. Настройка Kafka Consumer
consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset='earliest', # Начинать чтение с самых старых сообщений
    enable_auto_commit=True, # Автоматически подтверждать обработку сообщений
    value_deserializer=lambda x: json.loads(x.decode('utf-8')), # Десериализация JSON
    group_id="user_events_group" # Уникальный идентификатор группы
)

# 2. Подключение к ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='user',
    password='strongpassword'
)

# 3. Создание таблицы в ClickHouse
client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, username);
""")

# 4. Обработка сообщений
print("Ожидание сообщений из Kafka...")
for message in consumer:
    data = message.value
    print(f"Получено сообщение: {data}")
    
    # Вставка данных в ClickHouse
    client.command(
        f"INSERT INTO user_logins (username, event_type, event_time) "
        f"VALUES ('{data['user']}', '{data['event']}', toDateTime({data['timestamp']}))"
    )
    print(f"Данные пользователя {data['user']} записаны в ClickHouse")

# 5. Закрытие соединений
consumer.close()
client.close()
print("Работа завершена")