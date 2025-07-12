from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_dag_appear",
    start_date=datetime(2025, 7, 30),
    schedule_interval="@daily",
    catchup=False
) as dag:
    start = EmptyOperator(task_id="start")

