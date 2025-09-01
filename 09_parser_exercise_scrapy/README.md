# 📊 Парсер + Очистка данных в PySpark

## 📋 Описание
Проект состоит из двух частей:
1. **Сбор данных** — парсер на Scrapy для сайта [585zolotoy.ru](https://www.585zolotoy.ru).
2. **Обработка и загрузка** — файл notebook для PySparkанализа, очистки и сохранения данных в PostgreSQL.

Таким образом, данные о товарах (в примере используется категория "Кольца") сначала собираются в "сыром" виде, затем проходят этап очистки и подготовки, и только потом загружаются в целевую таблицу в Postgres.

---

## 🛠 Технологии
- Python 3.8+
- Scrapy 2.11+
- PostgreSQL 12+
- PySpark 3.3+
- SQLAlchemy 1.4+
- Selenium 4.0+ (fallback)
- Fake-useragent 1.2+

---

## 📦 Установка
Клонируйте репозиторий:
```bash
git clone <repository-url>
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
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

---

## 🗄️ Схемы таблиц

### Сырая таблица (`zolotoy_raw_products`)
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

### 1. Парсинг данных
Запустите Scrapy-паука:
```bash
scrapy crawl zolotoy585_parser
```

### 2. Загрузка и очистка в PySpark
Основные шаги в PySpark:
- Загрузка спарсенных "сырых" данных из таблицы `zolotoy_raw_products`
- Разведочный анализ:
  - Восстановление `category`
  - Анализ `subcategory`
  - Очистка `price` и `old_price`
- Очистка и преобразование:
  - Приведение типов (`sku`, `price`, `discount`, `rating`, `reviews`)
  - Удаление лишних символов в `product_url` (`%7C`, `|`)
  - Форматирование даты вставки (`inserted_date` без миллисекунд)
- Запись в `zolotoy_clean_products`

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
