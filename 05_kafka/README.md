
# 🔁 Пайплайн: PostgreSQL → Kafka → ClickHouse

## 📌 Описание
Пайплайн обеспечивает надёжную и безопасную передачу пользовательских событий из таблицы user_logins в базе данных PostgreSQL в ClickHouse из через Apache Kafka. 
Механизм гарантирует защиту от дублирования записей с помощью поля `sent_to_kafka = TRUE` в PostgreSQL и `group_id` в Kafka Consumer.

---

## ⚙️ Архитектура
```
PostgreSQL → Kafka Producer → Kafka Broker → Kafka Consumer → ClickHouse
```

---

## 🧩 Компоненты

### 🔹 PostgreSQL (`producer.py`)
- Создает исходную таблицу `user_logins`
- Генерирует тестовые события (для заполнения исходной таблицы)
- Отправляет только новые события (где `sent_to_kafka = FALSE`) в Kafka
- Помечает записи как отправленные (`sent_to_kafka = TRUE`)

### 🔹 Kafka
- Топик: `user_events`
- Брокер по умолчанию: `localhost:9092`
- Используется как буфер обмена сообщениями

### 🔹 ClickHouse (`consumer.py`)
- Подписчик Kafka (`group_id=user_events_group`)
- Читает сообщения из Kafka и записывает в таблицу `user_logins`
- Создаёт таблицу при необходимости

---

## 🚀 Запуск

### 1. Запуск Kafka и Zookeeper
```bash
docker-compose up -d
```

### 2. Установка зависимостей
```bash
pip install kafka-python psycopg2-binary clickhouse-connect
```

### 3. Подготовка PostgreSQL
Создание таблицы:
```sql
CREATE TABLE user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
);
```

### 4. Запуск consumer
```bash
python consumer.py
```

### 5. Запуск producer
```bash
python producer.py
```

---

## ✅ Проверка

После запуска producer и consumer:
1. Данные отправляются в Kafka
2. Consumer сохраняет их в ClickHouse
3. Повторно producer не отправит уже обработанные записи

---

## 🧪 Структура проекта

```
.
├── producer.py
├── consumer.py
├── docker-compose.yml
└── README.md
```

---