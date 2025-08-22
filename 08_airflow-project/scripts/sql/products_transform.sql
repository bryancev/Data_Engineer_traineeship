-- schedule: @daily
-- Обновляем пустые категории
UPDATE products 
SET category = 'Unknown' 
WHERE category IS NULL OR category = '';

-- Преобразуем данные и загружаем в целевую таблицу
INSERT INTO products_transformed (id, name, price, category, price_category)
SELECT 
    id,
    name,
    price,
    COALESCE(category, 'Unknown'),
    CASE 
        WHEN price < 1000 THEN 'Budget'
        WHEN price BETWEEN 1000 AND 5000 THEN 'Medium'
        ELSE 'Premium'
    END
FROM products
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    category = EXCLUDED.category,
    price_category = EXCLUDED.price_category;