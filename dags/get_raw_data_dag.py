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

# Define directories
LANDING_DIR = "/opt/airflow/data/landing"
LOCAL_EXCEL_PATH = "/opt/airflow/data_sources/natural_disasters.xlsx"

# Function to download dataset from Kaggle
def download_kaggle_dataset():
    api = KaggleApi()
    api.authenticate()
    # Example: replace with your actual Kaggle dataset path
    dataset = "heesoo37/120-years-of-olympic-history-athletes-and-results"
    api.dataset_download_files(dataset, path=LANDING_DIR, unzip=True)

# Function to copy the local Excel file
def copy_excel_to_landing():
    os.makedirs(LANDING_DIR, exist_ok=True)
    destination = os.path.join(LANDING_DIR, os.path.basename(LOCAL_EXCEL_PATH))
    shutil.copy(LOCAL_EXCEL_PATH, destination)

# DAG definition
with DAG(
    dag_id="pipeline_ingest_raw_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["ingestion", "raw_data"],
) as dag:

    # Task 1: Download data from Kaggle
    download_kaggle = PythonOperator(
        task_id="download_kaggle_data",
        python_callable=download_kaggle_dataset,
    )

    # Task 2: Copy Excel file
    copy_excel = PythonOperator(
        task_id="copy_excel_file",
        python_callable=copy_excel_to_landing,
    )

    # Task 3: Verify landing zone
    verify = BashOperator(
        task_id="verify_landing_zone",
        bash_command=f"ls -lh {LANDING_DIR}"
    )

    # Define dependencies
    [download_kaggle, copy_excel] >> verify
