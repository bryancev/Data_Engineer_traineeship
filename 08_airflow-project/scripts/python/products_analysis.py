# schedule: @daily
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

def generate_products_report():
    """Генерация отчета по продуктам и сохранение в txt файл"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # Анализ данных с использованием чистого SQL
    cur.execute("""
        SELECT 
            COUNT(*) as total_products,
            AVG(price) as avg_price,
            category,
            COUNT(*) as count_per_category
        FROM products 
        GROUP BY category
    """)
    
    results = cur.fetchall()
    
    # Создание отчета
    total_products = 0
    total_price = 0
    category_counts = {}
    
    for row in results:
        total_products += row[0]
        if row[1]:  # Если avg_price не None
            total_price += row[1] * row[0]  # Взвешенное среднее
        category_counts[row[2] or 'Unknown'] = row[3]
    
    # Расчет среднего значения
    avg_price = total_price / total_products if total_products > 0 else 0
    
    report = f"""
    ОТЧЕТ ПО ПРОДУКТАМ
    ==================
    Дата отчета: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    Общее количество продуктов: {total_products}
    Средняя цена: {avg_price:.2f} руб.
    
    Распределение по категориям:
    """
    
    for category, count in category_counts.items():
        report += f"\n- {category}: {count} товаров"
    
    # Создаем директорию для отчетов, если ее нет
    reports_dir = "/opt/airflow/reports"
    os.makedirs(reports_dir, exist_ok=True)
    
    # Сохранение отчета в текстовый файл
    report_filename = f"products_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_path = os.path.join(reports_dir, report_filename)
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    
    print(f"Отчет по продуктам сохранен в: {report_path}")
    print(report)