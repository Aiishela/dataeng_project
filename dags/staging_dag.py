import os
from pathlib import Path
import unicodedata
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Paths
LANDING_DIR = "/opt/airflow/data/landing"
STAGING_DIR = "/opt/airflow/data/staging"

# ---------------------- FUNCTIONS ----------------------

# --- Load functions ---
def load_bio(ti):
    bio = pd.read_csv(Path(LANDING_DIR) / "Olympic_Athlete_Bio.csv")
    ti.xcom_push(key="bio_df", value=bio.to_json(orient="split"))

def load_res(ti):
    res = pd.read_csv(Path(LANDING_DIR) / "Olympic_Athlete_Event_Results.csv")
    ti.xcom_push(key="res_df", value=res.to_json(orient="split"))

def load_cou(ti):
    cou = pd.read_csv(Path(LANDING_DIR) / "Olympics_Country.csv")
    ti.xcom_push(key="cou_df", value=cou.to_json(orient="split"))

# --- Clean functions ---
def clean_bio(ti):
    bio = pd.read_json(ti.xcom_pull(key="bio_df", task_ids="load_bio"), orient="split")
    bio['athlete_id'] = pd.to_numeric(bio['athlete_id'], errors='coerce')
    bio = bio.drop_duplicates(subset=['athlete_id'], keep='first')
    bio['name'] = bio['name'].astype(str).str.strip()
    bio['name_norm'] = bio['name'].apply(lambda s: unicodedata.normalize('NFKC', s))
    bio['sex'] = bio['sex'].astype(str).str.upper().str.strip().replace({'Male':'M','Female':'F'})
    bio = bio.drop(columns=['born', 'height', 'weight', 'country', 'description', 'special_notes'])
    ti.xcom_push(key="bio_df_clean", value=bio.to_json(orient="split"))

def clean_res(ti):
    res = pd.read_json(ti.xcom_pull(key="res_df", task_ids="load_res"), orient="split")
    res['athlete_id'] = pd.to_numeric(res['athlete_id'], errors='coerce')
    res['edition_id'] = pd.to_numeric(res['edition_id'], errors='coerce')
    res['medal'] = res['medal'].astype(str).str.strip().replace({'': None, 'nan': None})
    res = res[(res['edition_id'] >= 1900) & (res['edition_id'] <= 2025)]
    res = res.drop(columns=['athlete', 'pos', 'isTeamSport'])
    ti.xcom_push(key="res_df_clean", value=res.to_json(orient="split"))

def clean_cou(ti):
    cou = pd.read_json(ti.xcom_pull(key="cou_df", task_ids="load_cou"), orient="split")
    cou = cou[~((cou['noc'] == 'ROC') & (cou['country'] == 'ROC'))]
    cou['country'] = cou['country'].astype(str).str.strip().str.upper()
    cou['noc'] = cou['noc'].astype(str).str.strip().str.upper()
    cou = pd.concat([cou, pd.DataFrame([{'noc': 'IFR', 'country': 'International Federation Representative Italy'}])], ignore_index=True)
    ti.xcom_push(key="cou_df_clean", value=cou.to_json(orient="split"))

# --- Save functions ---
def save_bio(ti):
    bio = pd.read_json(ti.xcom_pull(key="bio_df_clean", task_ids="clean_bio"), orient="split")
    bio.to_csv(Path(STAGING_DIR) / "Olympic_Athlete_Bio.csv", index=False)

def save_res(ti):
    res = pd.read_json(ti.xcom_pull(key="res_df_clean", task_ids="clean_res"), orient="split")
    res.to_csv(Path(STAGING_DIR) / "Olympic_Athlete_Event_Results.csv", index=False)

def save_cou(ti):
    cou = pd.read_json(ti.xcom_pull(key="cou_df_clean", task_ids="clean_cou"), orient="split")
    cou.to_csv(Path(STAGING_DIR) / "Olympics_Country.csv", index=False)

# ---------------------- DAG ----------------------

with DAG(
    dag_id="atomic_cleaning_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cleaning", "atomic_tasks"],
) as dag:

    create_staging_folder = BashOperator(
        task_id="create_staging_folder",
        bash_command=f"mkdir -p {STAGING_DIR}"
    )

    # Load tasks
    load_bio_task = PythonOperator(task_id="load_bio", python_callable=load_bio)
    load_res_task = PythonOperator(task_id="load_res", python_callable=load_res)
    load_cou_task = PythonOperator(task_id="load_cou", python_callable=load_cou)

    # Clean tasks
    clean_bio_task = PythonOperator(task_id="clean_bio", python_callable=clean_bio)
    clean_res_task = PythonOperator(task_id="clean_res", python_callable=clean_res)
    clean_cou_task = PythonOperator(task_id="clean_cou", python_callable=clean_cou)

    # Save tasks
    save_bio_task = PythonOperator(task_id="save_bio", python_callable=save_bio)
    save_res_task = PythonOperator(task_id="save_res", python_callable=save_res)
    save_cou_task = PythonOperator(task_id="save_cou", python_callable=save_cou)

    # ------------------ Dependencies ------------------

    create_staging_folder >> [load_bio_task, load_res_task, load_cou_task]

    load_bio_task >> clean_bio_task >> save_bio_task
    load_res_task >> clean_res_task >> save_res_task
    load_cou_task >> clean_cou_task >> save_cou_task
