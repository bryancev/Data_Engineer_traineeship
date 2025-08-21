
# 🛠️ Генерация DAG-сценариев в Airflow

## 📌 Описание

Проект реализует ETL-пайплайн с использованием Apache Airflow. Система загружает, преобразует и анализирует данные о продуктах, генерируя отчеты и проверяя качество данных.

## 📁 Структура проекта

```
airflow-products-etl/
├── dags/                  # DAG-файлы
├── scripts/               # SQL и Python-скрипты
├── data/                  # CSV/JSON данные
├── templates/             # Jinja2-шаблоны
├── reports/               # Итоговые отчеты
├── generate_dags.py       # Генератор DAG'ов
└── docker-compose.yml     # Конфигурация Docker
```

## 🚀 Быстрый старт

1. Установите Docker и Python 3.8+
2. Клонируйте репозиторий:
```bash
git clone <url>
cd airflow-products-etl
mkdir -p scripts/sql scripts/python data templates dags reports
```
3. Запустите Airflow:
```bash
docker-compose up -d
python generate_dags.py
```
4. Откройте [http://localhost:8080](http://localhost:8080), логин: `airflow`, пароль: `airflow`

## 🔄 Описание DAG'ов

| DAG                          | Описание                          |
|------------------------------|-----------------------------------|
| `sql_products_create_tables` | Создание таблиц (раз в неделю)    |
| `csv_products_load`          | Загрузка CSV в БД (ежедневно)     |
| `sql_products_transform`     | Преобразование данных (ежедневно) |
| `py_products_analysis`       | Анализ и отчет (ежедневно)        |
| `py_products_quality`        | Проверка качества (ежедневно)     |

## 🗃️ Пример таблиц

```sql
products (
    id INT PRIMARY KEY,
    name TEXT,
    price DECIMAL,
    category TEXT
)

products_transformed (
    id INT,
    name TEXT,
    price DECIMAL,
    category TEXT,
    price_category TEXT,
    created_at TIMESTAMP
)
```

## 📈 Пример отчетов
Сгенерированные отчеты сохраняются в папке `reports/`.

**Отчет по продуктам**
```
Всего: 10
Средняя цена: 8450.00 руб.
Категории:
- Электроника: 2
- Аксессуары: 4
- Аудио: 1
- Накопители: 2
- Unknown: 1
```

**Отчет по качеству**
```
Проблемных записей: 2
- ID 2: цена -1200
- ID 8: цена отсутствует
```

## ⚙️ Конфигурация Airflow

Создайте подключение `postgres_default`:
- Тип: PostgreSQL
- Хост: `pg_extra`, порт: `5432`
- БД: `postgres`
- Пользователь: `extradb_user`
- Пароль: `extradb_password`

## ⚙️ Генерация DAG'ов (`generate_dags.py`)

Скрипт автоматически создает DAG-файлы на основе:
- SQL-файлов (`scripts/sql/*.sql`)
- Python-скриптов (`scripts/python/*.py`)
- CSV-файлов с JSON-схемой (`data/*.csv` + `data/*.json`)

Пример запуска:
```bash
python generate_dags.py
```
Сгенерированные DAG'и сохраняются в папке `dags/`.

---

