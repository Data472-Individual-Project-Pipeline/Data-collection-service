import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from processors.col35_processor import Col35Processor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_col35_v1',
    default_args=default_args,
    description='A DAG to collect data from col35 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

api_url = 'http://3.106.55.200:5000/col35/prisonstatistics'
postgres_conn_id = 'postgres_data472'
owner = 'col35'


def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Col35Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
    processor.check_and_create_table()


def insert_data():
    logging.info("Inserting data")
    processor = Col35Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
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
