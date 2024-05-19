import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Import the custom data processor
from processors.tya51_processor import Tya51Processor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tya51_collection_pipeline_v3',
    default_args=default_args,
    description='A DAG to collect data from student tya51 datasets and insert into a Postgres database on AWS RDS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

location_url = 'http://13.236.194.130/tya51/api/river/location'
observation_base_url = 'http://13.236.194.130/tya51/api/river/observation'
owner = 'tya51'

def create_and_check_tables():
    logging.info("Creating and checking tables")
    processor = Tya51Processor(postgres_conn_id='postgres_data472')
    location_table_schema = """
    CREATE TABLE IF NOT EXISTS tya51_location (
        locationId VARCHAR PRIMARY KEY,
        name VARCHAR,
        nztmx INTEGER,
        nztmy INTEGER,
        type VARCHAR,
        unit VARCHAR,
        owner VARCHAR,
        inserted_at TIMESTAMP
    );
    """
    observation_table_schema = """
    CREATE TABLE IF NOT EXISTS tya51_observation (
        locationId VARCHAR,
        qualityCode VARCHAR,
        timestamp TIMESTAMP,
        value FLOAT,
        owner VARCHAR,
        inserted_at TIMESTAMP,
        PRIMARY KEY (locationId, timestamp)
    );
    """
    processor.create_tables(location_table_schema, observation_table_schema)

def insert_data():
    logging.info("Inserting data")
    processor = Tya51Processor(postgres_conn_id='postgres_data472')
    processor.process(location_url, observation_base_url, owner)

# Task to create and check tables
create_and_check_tables_task = PythonOperator(
    task_id='create_and_check_tables',
    python_callable=create_and_check_tables,
    dag=dag,
)

# Task to insert data
insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

# Set task dependencies
create_and_check_tables_task >> insert_data_task
