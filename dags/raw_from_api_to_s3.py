import logging

import duckdb
import pendulum
import requests
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "v.zavgorodnii"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("ACCESS_KEY")
SECRET_KEY = Variable.get("SECRET_KEY")

LONG_DESCRIPTION = """
# LONG DESCRITION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 24, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1)
}

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    api_url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'

    try:
        # Загружаем данные через requests
        logging.info(f"📥 Загружаем данные из {api_url}")
        response = requests.get(api_url)
        response.raise_for_status()

        # Читаем CSV в pandas
        logging.info("📊 Читаем CSV данные..")
        df = pd.read_csv(StringIO(response.text))
        logging.info(f"✅ Данные загружены, строк: {len(df)}")

        if len(df) == 0:
            logging.warning("⚠️ Нет данных для указанного периода")
            return
        
        # Сохраняем через DuckDB в S3
        con = duckdb.connect()

        con.sql(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;
        """)

        con.register('earthquake_data', df)

        # Сохраняем в S3
        logging.info("💾 Сохраняем в S3...")
        con.execute(f"""
            COPY earthquake_data 
            TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
            (FORMAT 'parquet', COMPRESSION 'gzip')
        """)

        con.close()
        logging.info(f"✅ Файл успешно сохранен в S3: {start_date}_00-00-00.gz.parquet")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Ошибка HTTP запроса: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Общая ошибка: {e}")
        raise

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end
