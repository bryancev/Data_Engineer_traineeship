-- Основная таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для хранения исторических изменений
CREATE TABLE IF NOT EXISTS users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- 1. Функция логирования изменений
CREATE OR REPLACE FUNCTION log_user_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.name IS DISTINCT FROM NEW.name THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;

    IF OLD.email IS DISTINCT FROM NEW.email THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;

    IF OLD.role IS DISTINCT FROM NEW.role THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
    END IF;

    -- Обновляем метку времени
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Триггер на таблицу users
DROP TRIGGER IF EXISTS users_audit_trigger ON users;
CREATE TRIGGER users_audit_trigger
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_update();

-- 3. Установка расширения pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 4. Функция экспорта данных в CSV
CREATE OR REPLACE FUNCTION export_users_audit_today()
RETURNS void AS $$
BEGIN
    EXECUTE format(
        'COPY (SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
          FROM users_audit
          WHERE changed_at::date = current_date)
          TO ''/tmp/users_audit_export_%s.csv''
          WITH (FORMAT CSV, HEADER)',
        to_char(now(), 'YYYY-MM-DD_HH24MI')
    );
END;
$$ LANGUAGE plpgsql;


-- 5. Настройка ежедневного экспорта в 3:00
SELECT cron.schedule('nightly_audit_export', '0 3 * * *', 'SELECT export_users_audit_today()');

----------------------------------------------------------------------
-- тестирование

-- вставим данные в таблицу users
INSERT INTO users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'reader'),
('Anna Petrova', 'anna@example.com', 'writer');

-- обновляем данные в таблице users

UPDATE users
SET email = 'ivan.new@example.com'
WHERE name = 'Ivan Ivanov';

UPDATE users
SET role = 'support'
WHERE name = 'Ivan Ivanov';

UPDATE users
SET name = 'Anna Smirnova
WHERE name = 'Anna Petrova';










