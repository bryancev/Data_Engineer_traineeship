-- schedule: @weekly
CREATE TABLE IF NOT EXISTS products (
    id INTEGER,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    category TEXT
);

CREATE TABLE IF NOT EXISTS products_transformed (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    category TEXT,
    price_category TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);