import os
import json
from datetime import datetime, timedelta
from pymongo import MongoClient

# 1. Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["some_database"]
events_collection = db["user_events"]
archive_collection = db["archived_users"]

# Очистка коллекций перед загрузкой (для тестирования)
events_collection.drop()
archive_collection.drop()

# 2. Список документов (для тестирования)
data = [
    {
        "user_id": 123,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 10, 0, 0),
        "user_info": {
            "email": "user1@example.com",
            "registration_date": datetime(2023, 12, 1, 10, 0, 0)
        }
    },
    {
        "user_id": 124,
        "event_type": "login",
        "event_time": datetime(2024, 1, 21, 9, 30, 0),
        "user_info": {
            "email": "user2@example.com",
            "registration_date": datetime(2023, 12, 2, 12, 0, 0)
        }
    },
    {
        "user_id": 125,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 19, 14, 15, 0),
        "user_info": {
            "email": "user3@example.com",
            "registration_date": datetime(2023, 12, 3, 11, 45, 0)
        }
    },
    {
        "user_id": 126,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 16, 0, 0),
        "user_info": {
            "email": "user4@example.com",
            "registration_date": datetime(2023, 12, 4, 9, 0, 0)
        }
    },
    {
        "user_id": 127,
        "event_type": "login",
        "event_time": datetime(2024, 1, 22, 10, 0, 0),
        "user_info": {
            "email": "user5@example.com",
            "registration_date": datetime(2023, 12, 5, 10, 0, 0)
        }
    },
    {
        "user_id": 128,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 22, 11, 30, 0),
        "user_info": {
            "email": "user6@example.com",
            "registration_date": datetime(2023, 12, 6, 13, 0, 0)
        }
    },
    {
        "user_id": 129,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 23, 15, 0, 0),
        "user_info": {
            "email": "user7@example.com",
            "registration_date": datetime(2023, 12, 7, 8, 0, 0)
        }
    },
    {
        "user_id": 130,
        "event_type": "login",
        "event_time": datetime(2024, 1, 23, 16, 45, 0),
        "user_info": {
            "email": "user8@example.com",
            "registration_date": datetime(2023, 12, 8, 10, 0, 0)
        }
    },
    {
        "user_id": 131,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 24, 12, 0, 0),
        "user_info": {
            "email": "user9@example.com",
            "registration_date": datetime(2023, 12, 9, 14, 0, 0)
        }
    },
    {
        "user_id": 132,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 24, 18, 30, 0),
        "user_info": {
            "email": "user10@example.com",
            "registration_date": datetime(2023, 12, 10, 10, 0, 0)
        }
    }
]

# Заливка данных в коллекцию (для тестирования)
events_collection.insert_many(data)

print("✅ Тестовые данные успешно загружены в MongoDB")


# 3. Поиск неактивных пользователей

# Текущая дата (без времени)
current_date = datetime.now().date()

# Дата 30 дней назад (для регистрации)
thirty_days_ago = current_date - timedelta(days=30)

# Дата 14 дней назад (для активности)
fourteen_days_ago = current_date - timedelta(days=14)

pipeline = [
    # Группируем события по пользователям
    {"$group": {
        "_id": "$user_id",
        "last_activity": {"$max": "$event_time"},
        "registration_date": {"$first": "$user_info.registration_date"}
    }},

    # Фильтруем по нашим условиям
    {"$match": {
        # Регистрация >30 дней назад
        "registration_date": {"$lt": datetime.combine(thirty_days_ago, datetime.min.time())},
        # Активность >14 дней назад
        "last_activity": {"$lt": datetime.combine(fourteen_days_ago, datetime.min.time())}
    }}
]

# Запрос к базе данных
not_active_users = list(events_collection.aggregate(pipeline))

# Получаем список ID неактивных пользователей
user_ids = [u["_id"] for u in not_active_users]

# 4. Перемещение неактивных пользователей в архив
if user_ids:
    to_archive = list(events_collection.find({"user_id": {"$in": user_ids}}))

    # Копирование в архивную коллекцию
    archive_collection.insert_many(to_archive)

    # Удаление из основной коллекции
    events_collection.delete_many({"user_id": {"$in": user_ids}})

# 5. Создание отчета
report = {
    "date": current_date.strftime("%Y-%m-%d"),
    "archived_users_count": len(user_ids),
    "archived_user_ids": user_ids
}

os.makedirs("reports", exist_ok=True)
filename = f"reports/archive_{report['date']}.json"
with open(filename, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

# 6. Вывод
print("=" * 40)
print(f"Дата архивации: {report['date']}")
print(f"Найдено неактивных пользователей: {report['archived_users_count']}")
if user_ids:
    print("ID:", ", ".join(map(str, user_ids)))
print(f"Отчет сохранен в {filename}")
print("=" * 40)
