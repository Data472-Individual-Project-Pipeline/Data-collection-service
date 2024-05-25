import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from processors.prisoner_processor import PrisonerProcessor 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_svi40_v1',
    default_args=default_args,
    description='A DAG to collect data from svi40 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

api_url_1 = 'http://3.107.58.175:5000/svi40/prisonstat'
api_url_2 = 'http://3.107.58.175:5000/svi40/prisonstatmeta'
postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID
owner = 'svi40'

def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = PrisonerProcessor(postgres_conn_id=postgres_conn_id, api_url=api_url_1)
    processor.check_and_create_tables()

def insert_data():
    logging.info("Inserting data")
    processor = PrisonerProcessor(postgres_conn_id=postgres_conn_id, api_url=api_url_1)
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
