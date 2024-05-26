import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from processors.are154_processor import Are154Processor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_are154_v1',
    default_args=default_args,
    description='A DAG to collect data from ARE154 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

api_url = 'http://13.211.229.32:5000/are154/dataapi'
postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID
owner = 'are154'


def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Are154Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
    processor.check_and_create_table()


def insert_data():
    logging.info("Inserting data")
    processor = Are154Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
    items = processor.fetch_data()
    processor.insert_items(items, owner)


create_and_check_tables_task = PythonOperator(
    task_id='create_and_check_tables',
    python_callable=create_and_check_tables,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

create_and_check_tables_task >> insert_data_task
