# Импорты из библиотек
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import os
import subprocess

# Определение default_args
default_args = {
    "owner": "l4244", 
    "depends_on_past": False, 
    "email": ["l4244@yandex.ru"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=1),
}

# Определение функции initialize_, которая инициализирует таблицы в базе данных Vertica
def initialize_(conn_info):
    # Установка соединения с базой данных Vertica
    vrt_conn = vertica_python.connect(**conn_info)
    # Создание объекта курсора для выполнения SQL-запросов
    vrt_cur = vrt_conn.cursor()

    # Чтение скрипта SQL из файла
    init_sql = open('/lessons/sql/init.sql').read()

    # Выполнение скрипта SQL для создания таблицы transactions
    vrt_cur.execute(init_sql)
    print('Init Complete')

# Определение DAG
with DAG(
    "Init_DAG",
    default_args=default_args,
    description="Initialize (or flush) tables in db",
    schedule_interval=None,
    start_date=days_ago(0),
    tags=["init"],
    catchup=False,
) as dag:
    # Установка двух сторонних библиотек "vertica_python" и "python-dotenv" при помощи команды "pip install"
    subprocess.run(['pip', 'install', 'vertica_python'])
    subprocess.run(['pip', 'install', 'python-dotenv'])

    # Импорт библиотек, которые были установлены ранее
    import vertica_python
    from dotenv import load_dotenv

    # Загрузка переменных окружения из файла .env
    load_dotenv()

    # Подключение к базе данных Vertica с использованием переменных окружения

    conn_info = {
        'host': os.environ['DB_HOST'],
        'port': os.environ['DB_PORT'],
        'user': os.environ['DB_USER'],
        'password': os.environ['DB_PASSWORD'],
        'database': os.environ['DB_NAME'],
        'autocommit': os.environ['DB_AUTOCOMMIT'] == 'True'
    }

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    init = PythonOperator(
        task_id="init",
        python_callable=initialize_,
        op_kwargs={'conn_info': conn_info}
    )

start >> init >> end