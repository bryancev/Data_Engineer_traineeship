# 📋 Аудит изменений пользователей в базе данных PostgreSQL

## 📘 Описание

Этот проект реализует систему аудита изменений пользователей в базе данных PostgreSQL. В компании не было системы отслеживания изменений по таблице `users`, и теперь необходимо:

- Логировать изменения в полях `name`, `email`, `role`;
- Хранить историю изменений в отдельной таблице `users_audit`;
- Экспортировать ежедневные изменения в формате CSV;
- Автоматизировать экспорт с помощью расширения `pg_cron`.

## 🏗 Структура таблиц

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);
```

## 🛠 Техническое задание

### 1. Создание функции логирования

Реализована функция, отслеживающая изменения в полях `name`, `email`, `role`, и записывающая их в таблицу `users_audit`.

### 2. Создание триггера

Добавлен `BEFORE UPDATE` триггер на таблицу `users`, который вызывает функцию логирования перед изменением данных.

### 3. Установка pg_cron

В образ Docker включено расширение `pg_cron`, которое необходимо активировать:

```sql
CREATE EXTENSION IF NOT EXISTS pg_cron;
```

### 4. Экспорт свежих данных

Реализована SQL-функция, которая:

- Извлекает данные из `users_audit`, где `changed_at` соответствует текущему дню;
- Сохраняет их в CSV-файл по пути `/tmp/users_audit_export_<дата>.csv`.

### 5. Планировщик

С помощью `pg_cron` настроен ежедневный экспорт в **03:00 ночи**:

```sql
SELECT cron.schedule(
    'daily_users_audit_export',
    '0 3 * * *',
    $$ SELECT export_today_users_audit(); $$
);
```

## ✅ Критерии выполнения

| Требование | Статус |
|-----------|--------|
| Расширение `pg_cron` установлено и активно | ✅ |
| Задача в `cron.job` настроена | ✅ |
| CSV-файл создаётся и доступен внутри Docker | ✅ |
| Функция логирования изменений реализована | ✅ |
| Триггер прикреплён к таблице `users` | ✅ |
| Все команды находятся в одном SQL-файле | ✅ |


## 📅 Результат

После запуска и настройки ежедневно в 03:00 в контейнере по пути `/tmp` будет появляться файл:

```
/tmp/users_audit_export_<YYYY-MM-DD_HH24MI>.csv
```

с изменениями пользователей за прошедший день.

---