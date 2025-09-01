# 📊 Парсер + Очистка данных в PySpark

## 📋 Описание
Проект состоит из двух частей:
1. **Сбор данных** — парсер на Scrapy для сайта [585zolotoy.ru](https://www.585zolotoy.ru).
2. **Обработка и загрузка** — файл notebook для PySpark анализа, очистки и сохранения данных в PostgreSQL.

Собранные данные о товарах (из категории "Кольца") первоначально сохраняются в PostgreSQL в виде "сырых" данных. Затем эти данные читаются, очищаются и записываются в новую таблицу PostgreSQL в обработанном виде.

---

## 🛠 Технологии
- Python 3.8+
- Scrapy 2.11+
- PostgreSQL 12+
- PySpark 3.3+
- SQLAlchemy 1.4+
- Selenium 4.0+
- Fake-useragent 1.2+

---

## 📦 Установка
Клонируйте репозиторий:
```bash
git clone
cd zolotoy585
```

Установите зависимости:
```bash
pip install -r requirements.txt
```

Установите драйверы для PySpark (например, `postgresql-42.x.x.jar` в папку `jars`).

---

## ⚙️ Настройка
Создайте файл `.env` в корне проекта:
```env
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database
POSTGRES_HOST=your_host
POSTGRES_PORT=your_port
```

---

### Очищенная таблица (`zolotoy_clean_products`)
```sql
CREATE TABLE public.zolotoy_clean_products (
    sku INTEGER UNIQUE PRIMARY KEY,
    category TEXT NOT NULL,
    subcategory TEXT,
    name TEXT NOT NULL,
    price INT,
    old_price INT,
    discount INT,
    rating INT NOT NULL,
    reviews INT NOT NULL,
    product_url TEXT NOT NULL,
    parsed_date DATE,
    inserted_date TIMESTAMP
);
```

---

## 🚀 Использование

### 1. Создание таблица `zolotoy_raw_products` в Postgres для хранения "сырых" данных
```sql
CREATE TABLE public.zolotoy_raw_products (
    sku TEXT,
    category TEXT,
    subcategory TEXT,
    name TEXT,
    price TEXT,
    old_price TEXT,
    discount TEXT,
    rating TEXT,
    reviews TEXT,
    product_url TEXT,
    parsed_date DATE
);
```

### 2. Парсинг данных
Запуск Scrapy-парсера:
```bash
scrapy crawl zolotoy585_parser
```

### 3. Загрузка и очистка в PySpark
Основные шаги в PySpark (файл zolotoy585_pySpark_ETL.ipynb):
- Загрузка спарсенных "сырых" данных из таблицы `zolotoy_raw_products`
- Разведочный анализ:
  - Восстановление `category`
  - Анализ `subcategory`
  - Очистка `price` и `old_price`
- Очистка и преобразование:
  - Приведение типов (`sku`, `price`, `discount`, `rating`, `reviews`)
  - Удаление лишних символов в `product_url` (`%7C`, `|`)
- Запись очищенных данных в таблицу `zolotoy_clean_products`

---

## 📊 Собираемые данные
- Артикул (SKU)  
- Категория и подкатегория  
- Название товара  
- Цена и старая цена  
- Скидка  
- Рейтинг и количество отзывов  
- URL товара  
- Дата парсинга  
- Дата вставки  

---

## 📁 Структура проекта
```
zolotoy585/
├── spiders/
│   └── zolotoy_spider.py          # Scrapy spider
├── zolotoy585_pySpark_ETL.ipynb   # Очистка и загрузка в PySpark
├── models/
│   └── raw_products_model.py      # SQLAlchemy модель
├── pipelines.py                   # Pipeline Scrapy
├── settings.py                    # Настройки Scrapy
├── requirements.txt               # Зависимости
└── .env.example                   # Пример env-файла
```

---

## 📌 Итог
1. Scrapy собирает и пишет "сырые" данные в таблицу Postges `zolotoy_raw_products`.  
2. PySpark анализирует, чистит и пишет данные в таблицу Postges `zolotoy_clean_products`.  
3. Данные готовы для аналитики и построения витрин.  
