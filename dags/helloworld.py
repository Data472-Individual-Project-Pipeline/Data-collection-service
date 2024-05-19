from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_date():
    print(f"Current date and time: {datetime.now()}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='simple_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 1),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    print_date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> print_date_task >> end
