import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from processors.hra80_processor import Hra80Processor
from processors.river_flow_db_processor import create_and_check_tables

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_collection_pipeline_hra80_v1',
    default_args=default_args,
    description='A DAG to collect data from student hra80 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

location_url = 'http://13.239.6.15:5000/get-document/river-flow-all-master-data'
observation_base_url = 'http://13.239.6.15:5000/get-document/river-flow-transaction-data-all?start_date=2024-04-20%2010:15:00&end_date=2024-04-20%2023:15:00'
owner = 'hra80'

# Task to create and check tables
create_and_check_tables_task = PythonOperator(
    task_id='create_and_check_tables',
    python_callable=create_and_check_tables,
    op_kwargs={'postgres_conn_id': 'postgres_data472'},
    dag=dag,
)

def insert_data():
    logging.info("Inserting data")
    processor = Hra80Processor(postgres_conn_id='postgres_data472')
    processor.process(location_url, observation_base_url, owner)

# Task to insert data
insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

# Set task dependencies
create_and_check_tables_task >> insert_data_task
