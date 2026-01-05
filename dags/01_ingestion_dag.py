from datetime import datetime
import shutil


from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator

import os

# ----------------------- PATHS -----------------------------------------------------
LANDING_DIR = "/opt/airflow/data/landing"       # landing directory where the raw data is put
LOCAL_EXCEL_PATH = "/opt/airflow/data_sources/natural_disasters_from_1900.xlsx" # path to the excel file for the natural disasters
KAGGLE_CREDENTIALS = "/home/airflow/.config/kaggle/kaggle.json"    # json file with the kaggle credentials
LOCAL_OLYMPIC_DIR = "/opt/airflow/data_sources/olympic"     # olympic dataset present locally

# ----------------------- FUNCTIONS -----------------------------------------------------
def create_landing_dir():
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR, exist_ok=True)

# Verifies if the Kaggle credentials are present
def kaggle_credentials_exist():
    return os.path.exists(KAGGLE_CREDENTIALS)

# Branches to download files from kaggle or take the local files
def choose_olympic_source():
    if kaggle_credentials_exist():
        return "download_kaggle_data"
    return "copy_local_olympic_data"

# Copies the local olympic dataset to the landing directory
def copy_local_olympic_data():
    for file in os.listdir(LOCAL_OLYMPIC_DIR):
        if file.endswith(".csv"):
            shutil.copy(
                os.path.join(LOCAL_OLYMPIC_DIR, file),
                LANDING_DIR
            )

# Downloads olympic dataset from Kaggle, containing 6 csv files, to the landing directory
# Read the README to find how to get the API Key
def download_kaggle_dataset():
    from kaggle.api.kaggle_api_extended import KaggleApi
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
    dag_id="01_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "raw_data"],
) as dag:
    
    create_landing_folder = PythonOperator(
        task_id="create_landing_dir",
        python_callable=create_landing_dir,
    )

    choose_source = BranchPythonOperator(
        task_id="choose_olympic_source",
        python_callable=choose_olympic_source,
    )

    copy_local_olympic = PythonOperator(
        task_id="copy_local_olympic_data",
        python_callable=copy_local_olympic_data,
    )

    join_sources = EmptyOperator(
        task_id="join_sources",
        trigger_rule="none_failed_min_one_success",
    )

    download_kaggle = PythonOperator(
        task_id="download_kaggle_data",
        python_callable=download_kaggle_dataset,
    )

    copy_local_disaster = PythonOperator(
        task_id="copy_local_disaster",
        python_callable=copy_excel_to_landing,
    )

    verify = BashOperator(
        task_id="verify_landing_zone",
        bash_command=f"ls -lh {LANDING_DIR}"
    )

    create_landing_folder >> choose_source

    choose_source >> download_kaggle >> join_sources
    choose_source >> copy_local_olympic >> join_sources

    join_sources >> copy_local_disaster >> verify
