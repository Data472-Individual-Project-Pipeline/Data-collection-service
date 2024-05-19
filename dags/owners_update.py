from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'owners_update_v1',
    default_args=default_args,
    description='A DAG to manually trigger the update_owners_dag',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2024, 5, 18),
    catchup=False,
)

trigger_update_owners_task = TriggerDagRunOperator(
    task_id='trigger_update_owners',
    trigger_dag_id='update_owners_dag',
    dag=dag,
)
