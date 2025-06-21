-- 1. Таблица для сранения "сырых" логов (30 дней хранения)
DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events (
    user_id      UInt32,
    event_type   String,
    points_spent UInt32,
    event_time   DateTime
) ENGINE = MergeTree()
  ORDER BY (event_time, user_id)
  TTL event_time + INTERVAL 30 DAY DELETE;

-- 2. Агрегированная таблица (180 дней хранения)
DROP TABLE IF EXISTS user_events_agg;
CREATE TABLE user_events_agg (
    event_date            Date,
    event_type            String,
    unique_users    AggregateFunction(uniqExact, UInt32),
    total_spent    AggregateFunction(sum, UInt32),
    total_actions   AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
  ORDER BY (event_date, event_type)
  TTL event_date + INTERVAL 180 DAY DELETE;

-- 3. Материализованное представление: наполняет user_events_agg
DROP VIEW IF EXISTS user_events_mv;
CREATE MATERIALIZED VIEW user_events_mv
TO user_events_agg AS
SELECT
    toDate(event_time)               AS event_date,
    event_type,
    uniqExactState(user_id)          AS unique_users,
    sumState(points_spent)           AS total_spent,
    countState()                     AS total_actions
FROM user_events
GROUP BY event_date, event_type;

-- Тестовые данные
INSERT INTO user_events VALUES
    (1,'login',0, now() - INTERVAL 10 DAY),
    (2,'signup',0, now() - INTERVAL 10 DAY),
    (3,'login',0, now() - INTERVAL 10 DAY),
    (1,'login',0, now() - INTERVAL 7 DAY),
    (2,'login',0, now() - INTERVAL 7 DAY),
    (3,'purchase',30, now() - INTERVAL 7 DAY),
    (1,'purchase',50, now() - INTERVAL 5 DAY),
    (2,'logout',0, now() - INTERVAL 5 DAY),
    (4,'login',0, now() - INTERVAL 5 DAY),
    (1,'login',0, now() - INTERVAL 3 DAY),
    (3,'purchase',70, now() - INTERVAL 3 DAY),
    (5,'signup',0, now() - INTERVAL 3 DAY),
    (2,'purchase',20, now() - INTERVAL 1 DAY),
    (4,'logout',0, now() - INTERVAL 1 DAY),
    (5,'login',0, now() - INTERVAL 1 DAY),
    (1,'purchase',25, now()),
    (2,'login',0, now()),
    (3,'logout',0, now()),
    (6,'signup',0, now()),
    (6,'purchase',100, now());


-- 4. 7-дневный retention
WITH
    -- день первой активности
    first_visit AS (
        SELECT
            user_id,
            min(toDate(event_time)) AS cohort_date
        FROM user_events
        GROUP BY user_id
    )

SELECT
    f.cohort_date AS event_date,
    countDistinct(f.user_id) AS total_users_day_0,
    countDistinctIf(e.user_id, dateDiff('day', f.cohort_date, toDate(e.event_time)) BETWEEN 1 AND 7) AS returned_in_7_days,
    round(
        100.0 * countDistinctIf(e.user_id, dateDiff('day', f.cohort_date, toDate(e.event_time)) BETWEEN 1 AND 7)
        	/ countDistinct(f.user_id), 2
    ) AS retention_7d_percent
    
FROM first_visit AS f
LEFT JOIN user_events AS e
ON f.user_id = e.user_id

GROUP BY f.cohort_date
ORDER BY f.cohort_date;


-- 5. Запрос по быстрой аналитике по дням: разворачивание агрегатов
SELECT
    event_date,
    event_type,
    uniqExact(unique_users)  		AS unique_users,
    sumMerge(total_spent)        	AS total_spent,
    countMerge(total_actions)		AS total_actions
    
FROM user_events_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
