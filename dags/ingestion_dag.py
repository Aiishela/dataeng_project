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

import requests, logging, random, csv
from kaggle.api.kaggle_api_extended import KaggleApi
import os

# Define directories
LANDING_DIR = "/opt/airflow/data/landing"
LOCAL_EXCEL_PATH = "/opt/airflow/data_sources/natural_disasters_from_1900.xlsx"

def create_landing_folder():
    os.makedirs(LANDING_DIR, exist_ok=True)

# Function to download dataset from Kaggle
def download_kaggle_dataset():
    api = KaggleApi()
    api.authenticate()
    dataset = "josephcheng123456/olympic-historical-dataset-from-olympediaorg"
    api.dataset_download_files(dataset, path=LANDING_DIR, unzip=True)

# Function to copy the local Excel file
def copy_excel_to_landing():
    destination = os.path.join(LANDING_DIR, os.path.basename(LOCAL_EXCEL_PATH))
    shutil.copy(LOCAL_EXCEL_PATH, destination)

# DAG definition
with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "raw_data"],
) as dag:
    
    create_landing_zone = PythonOperator(
        task_id="create_landing_folder",
        python_callable=create_landing_folder,
    )

    download_kaggle = PythonOperator(
        task_id="download_kaggle_data",
        python_callable=download_kaggle_dataset,
    )

    copy_excel = PythonOperator(
        task_id="copy_excel_file",
        python_callable=copy_excel_to_landing,
    )

    verify = BashOperator(
        task_id="verify_landing_zone",
        bash_command=f"ls -lh {LANDING_DIR}"
    )

    # Define dependencies
    create_landing_zone >> [download_kaggle, copy_excel] >> verify
