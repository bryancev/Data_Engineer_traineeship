import os
import json
import re
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

# Конфигурация
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
SQL_SCRIPTS_DIR = os.path.join(SCRIPTS_DIR, "sql")
PYTHON_SCRIPTS_DIR = os.path.join(SCRIPTS_DIR, "python")
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
DAGS_DIR = os.path.join(BASE_DIR, "dags")
CONN_ID = "postgres_default"

# Пути внутри Docker контейнера Airflow
CONTAINER_SCRIPTS_DIR = "/opt/airflow/scripts"
CONTAINER_DATA_DIR = "/opt/airflow/data"

def extract_schedule(content):
    """Извлекает расписание из первой строки контента"""
    first_line = content.strip().split('\n')[0]
    
    if '-- schedule:' in first_line:
        return first_line.split('-- schedule:')[1].strip()
    elif '# schedule:' in first_line:
        return first_line.split('# schedule:')[1].strip()
    
    return "@daily"

def split_sql_commands(content):
    """Разделяет SQL на отдельные команды"""
    lines = content.strip().split('\n')
    # Пропускаем первую строку с расписанием
    if lines and any(keyword in lines[0].lower() for keyword in ['schedule:', 'schedule：']):
        lines = lines[1:]
    
    content = '\n'.join(lines)
    
    # Улучшенное разделение SQL команд
    commands = []
    current_command = []
    in_quotes = False
    quote_char = None
    
    for char in content:
        if char in ['"', "'"]:
            if in_quotes and char == quote_char:
                in_quotes = False
                quote_char = None
            elif not in_quotes:
                in_quotes = True
                quote_char = char
            current_command.append(char)
        elif char == ';' and not in_quotes:
            # Завершаем команду
            command_str = ''.join(current_command).strip()
            if command_str:
                commands.append(command_str)
            current_command = []
        else:
            current_command.append(char)
    
    # Добавляем последнюю команду, если она есть
    if current_command:
        command_str = ''.join(current_command).strip()
        if command_str:
            commands.append(command_str)
    
    return commands

def find_python_functions(content):
    """Находит функций в Python скрипте"""
    lines = content.strip().split('\n')
    # Пропускаем первую строку с расписанием
    if lines and any(keyword in lines[0].lower() for keyword in ['schedule:', 'schedule：']):
        lines = lines[1:]
    
    content = '\n'.join(lines)
    functions = re.findall(r"def\s+(\w+)\s*\(", content)
    return functions

def format_date_for_dag():
    """Форматирует дату для использования в DAG"""
    now = datetime.now()
    return f"{now.year}, {now.month}, {now.day}"

def load_json_schema(json_path):
    """Загружает схему таблиции из JSON файла"""
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def generate_table_ddl(table_name, schema):
    """Генерирует DDL для создания таблицы на основе JSON схемы"""
    if not schema or "columns" not in schema:
        raise ValueError(f"Invalid schema for table {table_name}")
    
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    column_definitions = []
    
    # Используем схему из JSON
    for column, column_type in schema["columns"].items():
        column_definitions.append(f"    {column} {column_type}")
    
    ddl += ",\n".join(column_definitions)
    ddl += "\n);"
    
    # Добавляем индексы если они указаны в схеме
    if "indexes" in schema:
        for index in schema["indexes"]:
            ddl += f"\n{index};"
    
    return ddl

def generate_dags():
    """Генерирует все DAG'и"""
    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    env.globals['enumerate'] = enumerate
    env.globals['range'] = range
    
    # Создаем папки если их нет
    for folder in [DAGS_DIR, DATA_DIR, SCRIPTS_DIR, TEMPLATES_DIR]:
        os.makedirs(folder, exist_ok=True)
    
    # Форматируем дату один раз
    start_date = format_date_for_dag()
    
    # SQL DAG'и
    sql_template = env.get_template("sql_dag_template.j2")
    for filename in os.listdir(SQL_SCRIPTS_DIR):
        if filename.endswith(".sql"):
            with open(os.path.join(SQL_SCRIPTS_DIR, filename), "r", encoding='utf-8') as f:
                content = f.read()
            
            commands = split_sql_commands(content)
            if not commands:
                continue
            
            # Используем единый шаблон для именования DAG
            dag_id = f"sql_{filename.replace('.sql', '')}"
                
            dag_code = sql_template.render(
                dag_id=dag_id,
                start_date=start_date,
                schedule=extract_schedule(content),
                commands=commands,
                conn_id=CONN_ID
            )
            
            with open(os.path.join(DAGS_DIR, f"{dag_id}.py"), "w", encoding='utf-8') as f:
                f.write(dag_code)
            print(f"Created SQL DAG: {dag_id}.py")
    
    # Python DAG'и
    py_template = env.get_template("python_dag_template.j2")
    for filename in os.listdir(PYTHON_SCRIPTS_DIR):
        if filename.endswith(".py"):
            with open(os.path.join(PYTHON_SCRIPTS_DIR, filename), "r", encoding='utf-8') as f:
                content = f.read()
            
            functions = find_python_functions(content)
            if not functions:
                continue
                
            # Используем путь внутри контейнера
            container_script_path = f"{CONTAINER_SCRIPTS_DIR}/python/{filename}"
            
            # Используем единый шаблон для именования DAG
            dag_id = f"py_{filename.replace('.py', '')}"
                
            dag_code = py_template.render(
                dag_id=dag_id,
                start_date=start_date,
                schedule=extract_schedule(content),
                functions=functions,
                script_path=container_script_path,
                module_name=filename.replace('.py', '')
            )
            
            with open(os.path.join(DAGS_DIR, f"{dag_id}.py"), "w", encoding='utf-8') as f:
                f.write(dag_code)
            print(f"Created Python DAG: {dag_id}.py")
    
    # CSV DAG'и
    csv_template = env.get_template("csv_dag_template.j2")
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            table_name = filename.replace('.csv', '')
            json_path = os.path.join(DATA_DIR, f"{table_name}.json")
            
            # Проверяем наличие JSON файла
            if not os.path.exists(json_path):
                print(f"Warning: JSON schema not found for {filename}, skipping...")
                continue
                
            # Загружаем схему из JSON
            schema = load_json_schema(json_path)
            
            # Генерируем DDL на основе JSON схемы
            ddl_content = generate_table_ddl(table_name, schema)
            
            # Используем путь внутри контейнера
            container_csv_path = f"{CONTAINER_DATA_DIR}/{filename}"
            
            # Используем единый шаблон для именования DAG
            dag_id = f"csv_{table_name}_load"
                
            dag_code = csv_template.render(
                dag_id=dag_id,
                start_date=start_date,
                schedule="@daily",
                table_name=table_name,
                csv_path=container_csv_path,
                ddl_sql=ddl_content,
                conn_id=CONN_ID
            )
            
            with open(os.path.join(DAGS_DIR, f"{dag_id}.py"), "w", encoding='utf-8') as f:
                f.write(dag_code)
            print(f"Created CSV DAG: {dag_id}.py")

if __name__ == "__main__":
    generate_dags()
    print("Генерация завершена!")