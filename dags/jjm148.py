import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from processors.jjm148_processor import Jjm148Processor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_jjm148_v1',
    default_args=default_args,
    description='A DAG to collect data from JJM148 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

api_url_1 = 'https://australia-southeast1-ecanairquality.cloudfunctions.net/airqualityapi'
api_url_2 = 'https://australia-southeast1-ecanairquality.cloudfunctions.net/airqualityapimeta'
postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID
owner = 'jjm148'


def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Jjm148Processor(postgres_conn_id=postgres_conn_id, api_url=api_url_1)
    processor.check_and_create_tables()


def insert_data():
    logging.info("Inserting data")
    processor = Jjm148Processor(postgres_conn_id=postgres_conn_id, api_url=api_url_1)
    items = processor.fetch_data(api_url=api_url_1)
    processor.insert_items(items, owner, 'jjm148_aqi')

    meta_data = processor.fetch_data(api_url=api_url_2)
    processor.insert_items(meta_data['CL_Stations'], owner, 'jjm148_CL_Stations')
    processor.insert_items(meta_data['CL_MonitorTypes'], owner, 'jjm148_CL_MonitorTypes')


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
