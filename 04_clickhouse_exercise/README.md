
# 📊 ClickHouse OLAP Mini-Project: User Events & Retention

## 🧩 Цель

Показать, как ClickHouse применяется как **OLAP база** в продуктовой аналитике, построить метрики и ретеншн на реальных пользовательских событиях (`user_events`).

## 🏗️ Что реализовано

### ✅ 1. Сырая таблица `user_events`
- Хранит пользовательские события.
- Удаляет данные старше 30 дней через TTL.
- Поля: `user_id`, `event_type`, `points_spent`, `event_time`.

### ✅ 2. Агрегированная таблица `user_events_agg`
- Хранит агрегаты на 180 дней.
- Использует агрегатные функции состояния:
  - `uniqExactState()` — уникальные пользователи,
  - `sumState()` — сумма баллов,
  - `countState()` — количество событий.
- Включает `cohort_date` для расчёта ретеншна.
- Хранит `user_ids_state` через `groupUniqArrayState()` для ретеншн-анализа.

### ✅ 3. Materialized View `user_events_mv`
- Автоматически агрегирует данные при вставке в `user_events`.
- Группирует по `(cohort_date, event_date, event_type)`.
- Заполняет агрегатную таблицу `user_events_agg`.

## 📈 Реализованные аналитики

### 📊 Быстрая аналитика по дням

```sql
SELECT
    event_date,
    event_type,
    uniqExact(unique_users_state)   AS unique_users,
    sumMerge(points_spent_state)    AS total_spent,
    countMerge(actions_count_state) AS total_actions
FROM user_events_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
```

### 📆 Retention (7 дней)

```sql
SELECT
    cohort.cohort_date,
    arrayUniq(cohort.users) AS total_users_day_0,
    arrayUniq(arrayIntersect(cohort.users, returned.users)) AS returned_in_7_days,
    round(arrayUniq(arrayIntersect(cohort.users, returned.users)) / arrayUniq(cohort.users) * 100, 2) AS retention_7d_percent
FROM (
    SELECT cohort_date, groupUniqArrayMerge(user_ids_state) AS users
    FROM user_events_agg
    WHERE cohort_date = event_date
    GROUP BY cohort_date
) AS cohort
LEFT JOIN (
    SELECT cohort_date, groupUniqArrayMerge(user_ids_state) AS users
    FROM user_events_agg
    WHERE dateDiff('day', cohort_date, event_date) BETWEEN 1 AND 7
    GROUP BY cohort_date
) AS returned USING (cohort_date)
ORDER BY cohort_date;
```

## 🧪 Тестовые данные

Запрос для вставки тестовых данных:

```sql
INSERT INTO user_events VALUES
-- События 10 дней назад
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

-- События 7 дней назад
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

-- События 5 дней назад
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

-- События 3 дня назад
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),

-- События вчера
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),

-- События сегодня
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());
```
