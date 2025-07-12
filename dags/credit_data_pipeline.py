from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from scripts.scrape_bi_excel import download_excel_and_upload_minio
from scripts.transform_kredit_data import transform_excel_minio_to_parquet
from scripts.transform_mom_flag import transform_mom_and_flag
from scripts.load_to_postgres import load_fact_credit_growth_clean
from scripts.transform_clean_fact_table import cleanup_and_load_fact_clean

with DAG(
    dag_id="credit_data_pipeline",
    start_date=datetime(2024, 8, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["credit", "etl"]
) as dag:

    step_1_download = PythonOperator(
        task_id="download_and_upload_minio",
        python_callable=download_excel_and_upload_minio
    )

    step_2_transform_combine = PythonOperator(
        task_id="transform_excel_to_parquet",
        python_callable=transform_excel_minio_to_parquet
    )

    step_3_transform_mom = PythonOperator(
        task_id="transform_mom_and_flag",
        python_callable=transform_mom_and_flag
    )

    step_5_load_raw = PythonOperator(
    task_id="load_fact_credit_growth_clean",
    python_callable=load_fact_credit_growth_clean
    )

    step_6_clean_fix = PythonOperator(
    task_id="load_and_clean_fact",
    python_callable=cleanup_and_load_fact_clean
    )
      

    
    # DAG Flow
    (
        step_1_download
        >> step_2_transform_combine
        >> step_3_transform_mom
        >> step_5_load_raw
        >> step_6_clean_fix
       
    )
