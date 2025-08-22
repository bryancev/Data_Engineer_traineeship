import os
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

# Пути внутри Docker контейнера
CONTAINER_SCRIPTS_DIR = "/opt/airflow/scripts"
CONTAINER_DATA_DIR = "/opt/airflow/data"

def extract_schedule(content):
    """Извлекает расписание из содержимого скрипта"""
    first_line = content.strip().split('\n')[0]
    
    if '-- schedule:' in first_line:
        return first_line.split('-- schedule:')[1].strip()
    elif '# schedule:' in first_line:
        return first_line.split('# schedule:')[1].strip()
    
    return "@daily"  # Расписание по умолчанию

def split_sql_commands(content):
    """Разделяет SQL на отдельные команды, игнорируя точки с запятой внутри кавычек"""
    lines = content.strip().split('\n')
    
    # Пропускаем первую строку если она содержит расписание
    if lines and any(keyword in lines[0].lower() for keyword in ['schedule:', 'schedule：']):
        lines = lines[1:]
    
    content = '\n'.join(lines)
    
    commands = []          # Список для хранения готовых SQL команд
    current_command = ""   # Текущая собираемая команда
    in_quotes = False      # Флаг нахождения внутри кавычек
    quote_char = None      # Тип кавычек (' или "), внутри которых находимся
    
    # Обрабатываем каждый символ в содержимом
    for char in content:
        if char in ['"', "'"]:
            # Обработка кавычек
            if in_quotes and char == quote_char:
                # Выходим из кавычек, если встречаем закрывающие кавычки того же типа
                in_quotes = False
                quote_char = None
            elif not in_quotes:
                # Входим в кавычки
                in_quotes = True
                quote_char = char
            current_command += char
        elif char == ';' and not in_quotes:
            # Находим точку с запятой вне кавычек - завершаем команду
            if current_command.strip():
                commands.append(current_command.strip())
            current_command = ""
        else:
            # Добавляем символ к текущей команде
            current_command += char
    
    # Добавляем последнюю команду, если она есть (на случай отсутствия точки с запятой в конце)
    if current_command.strip():
        commands.append(current_command.strip())
    
    return commands

def find_python_functions(content):
    """Находит функции в Python скрипте"""
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
            print(f"Создан SQL DAG: {dag_id}.py")
    
    # Python DAG'и
    py_template = env.get_template("python_dag_template.j2")
    for filename in os.listdir(PYTHON_SCRIPTS_DIR):
        if filename.endswith(".py"):
            with open(os.path.join(PYTHON_SCRIPTS_DIR, filename), "r", encoding='utf-8') as f:
                content = f.read()
            
            functions = find_python_functions(content)
            if not functions:
                continue
                
            container_script_path = f"{CONTAINER_SCRIPTS_DIR}/python/{filename}"
            
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
            
if __name__ == "__main__":
    generate_dags()
    print("Генерация DAG завершена!")