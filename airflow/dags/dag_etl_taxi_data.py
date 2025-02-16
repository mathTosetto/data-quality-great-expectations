import os
import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from main import main

logger: logging.Logger = logging.getLogger("dag_etl_taxi_data")


args = {"owner": "airflow", "depends_on_past": False, "email_on_failure": False}

dag = DAG(
    "TAXI",
    default_args=args,
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["taxi"],
)

with dag:
    task_1 = PythonOperator(
        task_id="load_taxi_data", python_callable=main, dag=dag, provide_context=True
    )
