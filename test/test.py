# dags/atomic_cleaning_dag.py
from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Paths (must be shared across workers)
LANDING_DIR = Path("/opt/airflow/data/landing")
STAGING_DIR = Path("/opt/airflow/data/staging")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

def _ensure_dirs():
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

# ---------- TASKS: cleaning functions (imports inside to speed up DAG import) ----------
def clean_games_task_fn(**context):
    import pandas as pd

    src = LANDING_DIR / "Olympics_Games.csv"
    dst = STAGING_DIR / "Olympics_Games.csv"

    if not src.exists():
        raise FileNotFoundError(f"games source not found: {src}")

    logging.info("Reading games from %s", src)
    games = pd.read_csv(src)

    logging.info("Cleaning games: numeric IDs, medal normalization, edition filter")
    games=games[games['competition_date']!='—']
    # Step 1: Split by EN DASH
    split_dates = games['competition_date'].str.split('–', expand=True)
    start = split_dates[0].str.strip()
    end = split_dates[1].str.strip()

    # Step 2: Extract day and month from start
    games['start_day'] = start.str.extract(r'(\d+)')  # digits
    games['start_month'] = start.str.extract(r'([A-Za-z]+)')  # letters

    # Step 3: Extract day and month from end
    games['end_day'] = end.str.extract(r'(\d+)')
    games['end_month'] = end.str.extract(r'([A-Za-z]+)')

    # Step 4: Fill missing start_month with end_month
    games['start_month'] = games['start_month'].fillna(games['end_month'])

    games['start_day'] = games['start_day'].astype(int)
    games['end_day'] = games['end_day'].astype(int)

    games['start_month_num'] = pd.to_datetime(games['start_month'], format='%B').dt.month
    games['end_month_num'] = pd.to_datetime(games['end_month'], format='%B').dt.month

    games['competition_start_date'] = pd.to_datetime(
    dict(year=games['year'],
         month=games['start_month_num'],
         day=games['start_day'])
)

    games['competition_end_date'] = pd.to_datetime(
        dict(year=games['year'],
            month=games['end_month_num'],
            day=games['end_day'])
)


    drop_cols = ['edition', 'edition_url', 'year', 'city', 'start_date','end_date','start_day','end_day','isHeld','start_month','end_month','start_month_num','end_month_num','competition_date','country_flag_url']
    existing_drop = [c for c in drop_cols if c in games.columns]
    if existing_drop:
        games = games.drop(columns=existing_drop)

    logging.info("Writing cleaned games to %s", dst)
    games.to_csv(dst, index=False)
def clean_bio_task_fn(**context):
    import pandas as pd
    import unicodedata

    src = LANDING_DIR / "Olympic_Athlete_Bio.csv"
    dst = STAGING_DIR / "Olympic_Athlete_Bio.csv"

    if not src.exists():
        raise FileNotFoundError(f"bio source not found: {src}")

    logging.info("Reading bio from %s", src)
    bio = pd.read_csv(src)

    logging.info("Cleaning bio: normalize ids, names, sex and drop unused cols")
    bio['athlete_id'] = pd.to_numeric(bio['athlete_id'], errors='coerce')
    bio = bio.drop_duplicates(subset=['athlete_id'], keep='first')
    bio['name'] = bio['name'].astype(str).str.strip()
    bio['name'] = bio['name'].apply(lambda s: unicodedata.normalize('NFKC', s))
    bio['sex'] = bio['sex'].astype(str).str.upper().str.strip().replace({'Male': 'M', 'Female': 'F'})

    drop_cols = ['born', 'height', 'weight', 'country', 'description', 'special_notes']
    existing_drop = [c for c in drop_cols if c in bio.columns]
    if existing_drop:
        bio = bio.drop(columns=existing_drop)

    logging.info("Writing cleaned bio to %s", dst)
    bio.to_csv(dst, index=False)


def clean_res_task_fn(**context):
    import pandas as pd

    src = LANDING_DIR / "Olympic_Athlete_Event_Results.csv"
    dst = STAGING_DIR / "Olympic_Athlete_Event_Results.csv"

    if not src.exists():
        raise FileNotFoundError(f"results source not found: {src}")

    logging.info("Reading results from %s", src)
    res = pd.read_csv(src)

    logging.info("Cleaning results: numeric IDs, medal normalization, edition filter")
    res['athlete_id'] = pd.to_numeric(res['athlete_id'], errors='coerce')
    res['edition_id'] = pd.to_numeric(res['edition_id'], errors='coerce')
    if 'medal' in res.columns:
        res['medal'] = res['medal'].astype(str).str.strip().replace({'': None, 'nan': None})


    drop_cols = ['athlete', 'pos', 'isTeamSport']
    existing_drop = [c for c in drop_cols if c in res.columns]
    if existing_drop:
        res = res.drop(columns=existing_drop)

    logging.info("Writing cleaned results to %s", dst)
    res.to_csv(dst, index=False)


def clean_cou_task_fn(**context):
    import pandas as pd

    src = LANDING_DIR / "Olympics_Country.csv"
    dst = STAGING_DIR / "Olympics_Country.csv"

    if not src.exists():
        raise FileNotFoundError(f"country source not found: {src}")

    logging.info("Reading countries from %s", src)
    cou = pd.read_csv(src)

    # Remove duplicated ROC row as you wanted
    if {'noc', 'country'}.issubset(cou.columns):
        cou = cou[~((cou['noc'] == 'ROC') & (cou['country'] == 'ROC'))]

    cou['country'] = cou['country'].astype(str).str.strip().str.upper()
    cou['noc'] = cou['noc'].astype(str).str.strip().str.upper()

    # Add IFR if not present
    if 'IFR' not in set(cou['noc'].dropna().unique()):
        extra = pd.DataFrame([{
            'noc': 'IFR',
            'country': 'International Federation Representative Italy'
        }])
        cou = pd.concat([cou, extra], ignore_index=True)

    logging.info("Writing cleaned countries to %s", dst)
    cou.to_csv(dst, index=False)

# --------------- NATURAL DISASTERS ------------------------
def clean_natural_disasters():
    import pandas as pd

    INPUT_FILE = Path(LANDING_DIR) / "natural_disasters_from_1900.xlsx"

    df = pd.read_excel(INPUT_FILE)  
    cols_to_drop = [
        "Historic",
        "Classification Key",
        "External IDs",
        "OFDA/BHA Response",
        "Appeal",
        "Declaration",
        "AID Contribution ('000 US$)",
        "Associated Types",
        # "Magnitude",
        # "Magnitude Scale",
        "River Basin",

        "Total Deaths",
        "No. Injured",
        "No. Affected",
        "No. Homeless",
        "Total Affected",
        "Reconstruction Costs ('000 US$)",
        "Reconstruction Costs, Adjusted ('000 US$)",
        "Insured Damage ('000 US$)",
        "Insured Damage, Adjusted ('000 US$)",
        "Total Damage ('000 US$)",
        "Total Damage, Adjusted ('000 US$)",

        "CPI",
        "Admin Units",
        "Entry Date",
        "Last Update"
    ]

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # df.to_excel("../data/test/01_remove_columns.xlsx", index=False)
    # -------------------------------- Normalize date 

    def safe_date(y, m, d):
        """Constructs a safe datetime from Y/M/D even if missing."""
        try:
            if pd.isna(y):
                return None
            y = int(y)
            m = int(m) if not pd.isna(m) else 1
            d = int(d) if not pd.isna(d) else 1
            return datetime(y, m, d)
        except:
            return None

    df["Start Date"] = df.apply(
        lambda r: safe_date(r["Start Year"], r["Start Month"], r["Start Day"]), axis=1
    )

    df["End Date"] = df.apply(
        lambda r: safe_date(r["End Year"], r["End Month"], r["End Day"]), axis=1
    )

    # Remove events where the date is invalid
    df = df[df["Start Date"].notna()]
    df = df[df["End Date"].notna()]

    # Keep reasonable years only
    df = df[(df["Start Year"] >= 1987) & (df["Start Year"] <= datetime.now().year)]

    # -------------------------------- Remove date columns 
    cols_to_drop = [
        "Start Year",
        "Start Month",
        "Start Day",
        "End Year",
        "End Month",
        "End Day"
    ]

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # df.to_excel(STAGING_DIR + "/02_normalize_date.xlsx", index=False)
    df.to_csv(Path(STAGING_DIR) / "02_normalize_date.csv", index=False)

# ---------------------- NEO4J ----------------------

def run_query():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            MATCH (n)
            RETURN count(n) AS nodes;
        """)

# ---------------------- DAG ----------------------

with DAG(
    dag_id="atomic_cleaning_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cleaning", "atomic"],
    schedule=None,  # run manually or trigger
) as dag:

    create_staging = PythonOperator(
        task_id="create_staging_folder",
        python_callable=_ensure_dirs,
        op_kwargs={},
    )

    clean_bio = PythonOperator(
        task_id="clean_bio",
        python_callable=clean_bio_task_fn,
        execution_timeout=timedelta(minutes=10),
    )

    clean_res = PythonOperator(
        task_id="clean_res",
        python_callable=clean_res_task_fn,
        execution_timeout=timedelta(minutes=15),
    )
    clean_games = PythonOperator(
        task_id="clean_games",
        python_callable=clean_games_task_fn,
        execution_timeout=timedelta(minutes=15),
    )

    clean_cou = PythonOperator(
        task_id="clean_cou",
        python_callable=clean_cou_task_fn,
        execution_timeout=timedelta(minutes=5),
    )

    clean_disasters = PythonOperator(
        task_id="clean_natural_disasters", 
        python_callable=clean_natural_disasters)

    run_cypher_disaster = PythonOperator(
        task_id="neo4j_query",
        python_callable=run_query
    )

    clean_games >> clean_disasters
    clean_disasters >> run_cypher_disaster >> create_staging

    # All cleaning tasks can run in parallel after staging folder exists
    create_staging >> [clean_bio, clean_games, clean_cou]
