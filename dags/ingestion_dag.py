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

# Paths used 
LANDING_DIR = "/opt/airflow/data/landing"       # landing directory where the raw data is put
LOCAL_EXCEL_PATH = "/opt/airflow/data_sources/natural_disasters_from_1900.xlsx" # path to the excel file for the natural disasters

# ----------------------- FUNCTIONS -----------------------------------------------------

# Download olympic dataset from Kaggle, containing 6 csv files, to the landing directory
# Read the README to find how to get the API Key
def download_kaggle_dataset():
    api = KaggleApi()
    api.authenticate()
    dataset = "josephcheng123456/olympic-historical-dataset-from-olympediaorg"
    api.dataset_download_files(dataset, path=LANDING_DIR, unzip=True)

# Copy the natural disasters excel files to the landing directory
def copy_excel_to_landing():
    destination = os.path.join(LANDING_DIR, os.path.basename(LOCAL_EXCEL_PATH))
    shutil.copy(LOCAL_EXCEL_PATH, destination)

# ----------------------- DAG -----------------------------------------------------

with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "raw_data"],
) as dag:
    
    create_landing_zone = BashOperator(
        task_id="create_landing_folder",
        bash_command=f"mkdir -p {LANDING_DIR}"
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

    create_landing_zone >> [download_kaggle, copy_excel] >> verify
