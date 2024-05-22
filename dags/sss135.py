import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from processors.sss135_processor import Sss135Processor 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_sss135_v1',
    default_args=default_args,
    description='A DAG to collect data from sss135 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

api_url_1 = 'http://3.107.51.162:5000/Shahron/prisonstatisticsapi'
api_url_2 = 'http://3.107.51.162:5000/Shahron/metadataapi'
postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID
owner = 'sss135'

def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Sss135Processor(postgres_conn_id=postgres_conn_id, api_url=api_url_1)
    processor.check_and_create_tables()

def insert_data():
    logging.info("Inserting data")
    processor = Sss135Processor(postgres_conn_id=postgres_conn_id, api_url=api_url_1)
    items = processor.fetch_data(api_url_1)
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
