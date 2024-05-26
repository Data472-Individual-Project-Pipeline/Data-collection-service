import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Adding the processors directory to the Python path
sys.path.append(os.path.dirname(__file__))

from processors.owner_processor import OwnerManager

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Individual_owners_create_v1',
    default_args=default_args,
    description='A DAG to create the owners table and insert initial data based on DAG files in the dags folder',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

postgres_conn_id = 'postgres_data472'  # Replace with your actual PostgreSQL connection ID
dags_folder = os.path.dirname(__file__)  # Path to the current DAGs folder


def create_and_check_table():
    logging.info("Creating and checking owners table")
    manager = OwnerManager(postgres_conn_id=postgres_conn_id, dags_folder=dags_folder)
    manager.check_and_create_table()


def update_owners():
    logging.info("Updating owners table")
    manager = OwnerManager(postgres_conn_id=postgres_conn_id, dags_folder=dags_folder)
    owner_names = manager.get_owner_names()
    manager.insert_owners(owner_names)


create_and_check_table_task = PythonOperator(
    task_id='create_and_check_table',
    python_callable=create_and_check_table,
    dag=dag,
)

update_owners_task = PythonOperator(
    task_id='update_owners',
    python_callable=update_owners,
    dag=dag,
)

create_and_check_table_task >> update_owners_task
