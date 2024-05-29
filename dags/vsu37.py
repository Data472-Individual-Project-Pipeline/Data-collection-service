import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Adding the processors directory to the Python path
sys.path.append(os.path.dirname(__file__))

from processors.vsu37_processor import Vsu37Processor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_vsu37_v1',
    default_args=default_args,
    description='A DAG to collect data from VSU37 rainfall datasets and insert into a Postgres database on AWS RDS',
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    start_date=datetime(2024, 5, 29),
    catchup=False,
)

api_url = 'http://13.239.119.236:5000/vsu37/rainfall_data'
postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID

def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Vsu37Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
    processor.check_and_create_table()

def insert_data():
    logging.info("Inserting data")
    processor = Vsu37Processor(postgres_conn_id=postgres_conn_id, api_url=api_url)
    items = processor.fetch_data()
    processor.insert_items(items)

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
