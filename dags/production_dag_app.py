import pendulum
from datetime import timedelta, datetime
import urllib.request as request
import pandas as pd
import shutil
import os
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator





# ----------------------- DAG -----------------------------------------------------

with DAG(
    dag_id="production_dag_app",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["production", "app"],
) as dag:
    
    dag_folder = os.path.dirname(os.path.realpath(__file__))
    app_folder = os.path.join(dag_folder, "..", "test")

    run_app = BashOperator(
        task_id='run_streamlit_dashboard',
        bash_command=f'cd {dag_folder}/app && streamlit run app.py',
    )
    run_app
