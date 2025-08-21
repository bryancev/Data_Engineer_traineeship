# schedule: @daily
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

def monitor_data_quality():
    """Мониторинг качества данных: проверка отрицательных и нулевых цен"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # Проверяем продукты с отрицательными или нулевыми ценами
    cur.execute("""
        SELECT 
            id,
            name,
            price,
            category
        FROM products 
        WHERE price IS NULL OR price <= 0
    """)
    
    problematic_products = cur.fetchall()
    
    # Создаем отчет о проблемных данных
    report = f"""
    ОТЧЕТ О КАЧЕСТВЕ ДАННЫХ
    =======================
    Дата отчета: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    Количество проблемных записей: {len(problematic_products)}
    
    Проблемные продукты (цена <= 0 или NULL):
    """
    
    if problematic_products:
        for product in problematic_products:
            product_id, name, price, category = product
            report += f"\n- ID: {product_id}, Название: {name}, Цена: {price}, Категория: {category or 'Не указана'}"
    else:
        report += "\nПроблемные записи не найдены. Все цены корректны."
    
    # Создаем директорию для отчетов, если ее нет
    reports_dir = "/opt/airflow/reports"
    os.makedirs(reports_dir, exist_ok=True)
    
    # Сохранение отчета в текстовый файл
    report_filename = f"products_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_path = os.path.join(reports_dir, report_filename)
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    
    print(f"Data quality report saved to: {report_path}")
    print(report)