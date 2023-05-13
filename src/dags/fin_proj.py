"""
Final_Proj_DE DAG
"""
import psycopg2
import vertica_python
from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import subprocess
import os

from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "l4244",
    "depends_on_past": True,
    "email": ["l4244@yandex.ru"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=1),
}


def stg_transactions(pg_cur, conn_info, date):
    # Export data from PostgreSQL to a CSV file
    select_query = open(
        '/lessons/sql/stg_select_transactions_query.sql').read().replace('{date}', date)

    with open(f'transactions-{date}.csv', 'w') as f:
        pg_cur.copy_expert(
            f''' COPY ({select_query}) TO STDOUT DELIMITER ',' ''', f)

    print(f'GOT PSQL DATA - {date}')

    # Import data from transactions CSV file to Vertica
    vrt_conn = vertica_python.connect(**conn_info)
    vrt_cur = vrt_conn.cursor()

    load_transactions_queries = open(
        '/lessons/sql/stg_load_transactions_to_vertica.sql').read().replace('{date}', date).split(';')
    for load_transactions_query in load_transactions_queries:

        vrt_cur.execute(load_transactions_query)

    vrt_conn.close()
    print('Stg transactions Complete')


def stg_currencies(pg_cur, conn_info, date):
    # Export data from PostgreSQL to a CSV file
    select_query = open(
        '/lessons/sql/stg_select_currencies_query.sql').read().replace('{date}', date)

    with open(f'currencies-{date}.csv', 'w') as f:
        pg_cur.copy_expert(
            f''' COPY ({select_query}) TO STDOUT DELIMITER ',' ''', f)

    print(f'GOT PSQL currencies DATA - {date}')

    # Import data from currencies CSV file to Vertica
    vrt_conn = vertica_python.connect(**conn_info)
    vrt_cur = vrt_conn.cursor()

    load_currencies_queries = open(
        '/lessons/sql/stg_load_currencies_to_vertica.sql').read().replace('{date}', date).split(';')
    for load_currencies_query in load_currencies_queries:

        vrt_cur.execute(load_currencies_query)

    vrt_conn.close()
    print('Stg currencies Complete')


def mart_global_metrics(conn_info, date):
    # Наполнение витрины

    vrt_conn = vertica_python.connect(**conn_info)
    vrt_cur = vrt_conn.cursor()

    cdm_global_metrics_queries = open(
        '/lessons/sql/cdm_global_metrics.sql').read().replace('{date}', date).split(';')
    for cdm_global_metrics_query in cdm_global_metrics_queries:

        vrt_cur.execute(cdm_global_metrics_query)

    vrt_conn.close()

    print('CDM global_metrics done')


with DAG(
    "Final_Proj_DE",
    default_args=default_args,
    description="Final_Proj_DE DAG",
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 1),
    tags=["fin", 'proj', 'kek'],
    catchup=True
) as dag:

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

    # настройки подключения
    DATABASE = os.environ['DATABASE']
    HOST = os.environ['HOST']
    PORT =os.environ['PORT']
    USER = os.environ['USER']
    PASSWORD = os.environ['PASSWORD']

    # путь к файлу с сертификатом
    SSLROOTCERT = "/lessons/dags/CA.pem"

    # создание подключения
    pg_conn = psycopg2.connect(
        database=DATABASE,
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        sslmode="verify-full",
        sslrootcert=SSLROOTCERT
    )

    # создание курсора
    pg_cur = pg_conn.cursor()

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    stg_transactions_task = PythonOperator(
        task_id="stg_transactions_task",
        python_callable=stg_transactions,
        op_kwargs={'pg_cur': pg_cur,
                   'conn_info': conn_info,
                   'date': '{{ ds }}'}
    )

    stg_currencies_task = PythonOperator(
        task_id="stg_currencies_task",
        python_callable=stg_currencies,
        op_kwargs={'pg_cur': pg_cur,
                   'conn_info': conn_info,
                   'date': '{{ ds }}'}
    )

    mart_global_metrics_task = PythonOperator(
        task_id="mart_global_metrics_task",
        python_callable=mart_global_metrics,
        op_kwargs={'conn_info': conn_info,
                   'date': '{{ ds }}'}
    )

start >> [stg_transactions_task,
          stg_currencies_task] >> mart_global_metrics_task >> end
