import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from processors.hwa205_processor import Hwa205Processor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_hwa205_v2',
    default_args=default_args,
    description='A DAG to collect data from HWA205 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

api_url = 'http://3.25.85.38:4000/graphql'
postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID
owner = 'hwa205'

def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Hwa205Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
    processor.check_and_create_table()

def insert_data():
    logging.info("Inserting data")
    processor = Hwa205Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
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
