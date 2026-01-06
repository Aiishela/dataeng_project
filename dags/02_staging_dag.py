from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ----------------------- PATHS -----------------------------------------------------
LANDING_DIR = Path("/opt/airflow/data/landing")
STAGING_DIR = Path("/opt/airflow/data/staging")

# ----------------------- CONST -----------------------------------------------------

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

CANONICAL_COUNTRY_MAP_COU = {
    # Yemen
    "NORTH YEMEN": "YEMEN",
    "SOUTH YEMEN": "YEMEN",

    # Germany
    "EAST GERMANY": "GERMANY",
    "WEST GERMANY": "GERMANY",
    "germany":"GERMANY",

    # Vietnam
    "SOUTH VIETNAM": "VIETNAM",
    
}
CANONICAL_NOC_MAP_COU = {
    # Yemen
    "YAR": "YEM",
    "YMD": "YEM",

    # Germany
    "GDR": "GER",
    "FRG": "GER",

    # Vietnam
    "VNM": "VIE",
}

CANONICAL_COUNTRY_MAP_ND = {
    # Germany
    "Germany Federal Republic": "GERMANY",
    "German Democratic Republic": "GERMANY",
    
    # Yemen
    "Yemen Arab Republic": "YEMEN",
    "People's Democratic Republic of Yemen": "YEMEN",

    # China
    "Taiwan (Province of China)": "PEOPLE'S REPUBLIC OF CHINA",
    "China, Macao Special Administrative Region": "PEOPLE'S REPUBLIC OF CHINA",
    "China": "PEOPLE'S REPUBLIC OF CHINA",

    # Netherlands
    "Netherlands (Kingdom of the)": "NETHERLANDS",

    
    # USA
    "United States of America": "UNITED STATES",

    # Vietnam
    "Viet Nam": "VIETNAM",
 
    #Guineau bissau
    "Guinea-Bissau":"GUINEA BISSAU",
    
    
}
CANONICAL_ISO_MAP_ND = {
    # Yemen
    "YMN": "YEM",
    "YMD": "YEM",

    # China
    "TWN": "CHN",
    "MAC": "CHN",
}

# ----------------------- FUNCTIONS -----------------------------------------------------
def create_staging_dir():
    import os
    if not os.path.exists(LANDING_DIR):
        os.makedirs(STAGING_DIR, exist_ok=True)

# ---------- CLEANING OF OLYMPIC DATASET ------------------------------------------------
# Each of the functions in this part cleans one of the olympic games dataset file
# The cleaning includes : normalization of names and dates, conversion of numbers and
# removal of unused columns
# The cleaned files are put inside the STAGING folder

def clean_games_task():
    import pandas as pd
    import os
    os.umask(0o022)

    src = LANDING_DIR / "Olympics_Games.csv"
    tmp_dst = STAGING_DIR / ".Olympics_Games.tmp.csv"
    final_dst = STAGING_DIR / "Olympics_Games.csv"

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

    games['country_noc'] = games['country_noc'].replace(CANONICAL_NOC_MAP_COU)

    drop_cols = [ 'edition_url', 'city', 'start_date','end_date','start_day','end_day','isHeld','start_month','end_month','start_month_num','end_month_num','competition_date','country_flag_url']
    existing_drop = [c for c in drop_cols if c in games.columns]
    if existing_drop:
        games = games.drop(columns=existing_drop)

    logging.info("Writing cleaned games to %s", tmp_dst)
    games.to_csv(tmp_dst, index=False)
    os.replace(tmp_dst, final_dst)

def clean_bio_task():
    import pandas as pd
    import unicodedata
    import os
    os.umask(0o022)

    src = LANDING_DIR / "Olympic_Athlete_Bio.csv"
    tmp_dst = STAGING_DIR / ".Olympic_Athlete_Bio.tmp.csv"
    final_dst = STAGING_DIR / "Olympic_Athlete_Bio.csv"

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

    drop_cols = [ 'height', 'weight', 'country', 'description', 'special_notes']
    existing_drop = [c for c in drop_cols if c in bio.columns]
    if existing_drop:
        bio = bio.drop(columns=existing_drop)
    bio['country_noc'] = bio['country_noc'].replace(CANONICAL_NOC_MAP_COU)

    logging.info("Writing cleaned bio to %s", tmp_dst)
    bio.to_csv(tmp_dst, index=False)
    os.replace(tmp_dst, final_dst)

def clean_res_task():
    import pandas as pd
    import os
    os.umask(0o022)

    src = LANDING_DIR / "Olympic_Athlete_Event_Results.csv"
    tmp_dst = STAGING_DIR / ".Olympic_Athlete_Event_Results.tmp.csv"
    final_dst = STAGING_DIR / "Olympic_Athlete_Event_Results.csv"

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

    res['country_noc'] = res['country_noc'].replace(CANONICAL_NOC_MAP_COU)
    res = res.drop_duplicates()
    logging.info("Writing cleaned results to %s", tmp_dst)
    res.to_csv(tmp_dst, index=False)
    os.replace(tmp_dst, final_dst)
    

def clean_cou_task():
    import pandas as pd
    import os
    os.umask(0o022)

    src = LANDING_DIR / "Olympics_Country.csv"
    final_dst = STAGING_DIR / "Olympics_Country.csv"
    tmp_dst = STAGING_DIR / ".Olympics_Country.tmp.csv"

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
    cou['noc'] = cou['noc'].replace(CANONICAL_NOC_MAP_COU)
    cou['country'] = cou['country'].replace(CANONICAL_COUNTRY_MAP_COU)
    cou=cou.drop_duplicates()
    logging.info("Writing cleaned countries to %s", final_dst)
    cou.to_csv(tmp_dst, index=False)
    os.replace(tmp_dst, final_dst)
    

# --------------- CLEANING OF NATURAL DISASTERS DATASET------------------------

def clean_natural_disasters():
    import pandas as pd
    import os
    import re
    import unicodedata
    os.umask(0o022)

    INPUT_FILE = Path(LANDING_DIR) / "natural_disasters_from_1900.xlsx"

    df = pd.read_excel(INPUT_FILE)  
    
    # -------------------------- Date normalization
    def safe_date(y, m, d):
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

    # Date columns to drop
    cols_to_drop = [
        "Start Year",
        "Start Month",
        "Start Day",
        "End Year",
        "End Month",
        "End Day"
    ]

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")
    df['Country'] = df['Country'].replace(CANONICAL_COUNTRY_MAP_ND)
    df['ISO'] = df['ISO'].replace(CANONICAL_ISO_MAP_ND)

    # --------------------------- Country Name Normalization
    dst_cou = STAGING_DIR / "Olympics_Country.csv"

    if not dst_cou.exists():
        raise FileNotFoundError(f"country source not found: {dst_cou}")

    logging.info("Reading countries from %s", dst_cou)
    cou = pd.read_csv(dst_cou)
    
    not_in_countries = df[~df['Country'].str.lower().isin(cou['country'].str.lower())]

    def words_set(name):
        if pd.isna(name):
            return None

        # Lowercase
        name = name.lower()

        # Remove accents
        name = unicodedata.normalize('NFKD', name)
        name = ''.join(c for c in name if not unicodedata.combining(c))

        # Replace separators with spaces
        name = re.sub(r'[-_/]', ' ', name)

        # Remove other punctuation
        name = re.sub(r'[^\w\s]', '', name)

        # Normalize whitespace
        name = re.sub(r'\s+', ' ', name).strip()

        return set(name.split())
    
    STOPWORDS = {'republic', 'of', 'the', 'and', 'is', 'democratic','arab','united','states','saint','islands','kingdom','new','peoples'}  # add more if needed

    cou_words = cou['country'].apply(words_set)
    not_in_words = not_in_countries['Country'].apply(words_set)

    matches = []

    for i, ni_words in enumerate(not_in_words):
        ni_name = not_in_countries.iloc[i]['Country']
        for j, c_words in enumerate(cou_words):
            c_name = cou.iloc[j]['country']
            shared = ni_words & c_words
            # ignore matches that are only stopwords
            filtered_shared = shared - STOPWORDS
            if filtered_shared:  # only keep if something meaningful remains
                matches.append((ni_name, c_name, filtered_shared))

    matches_df = pd.DataFrame(matches, columns=['NotIn', 'InCou', 'SharedWords'])
    matches_df['SharedWordsStr'] = matches_df['SharedWords'].apply(lambda x: ', '.join(sorted(x)))

    # Drop duplicates based on all columns or specific columns
    matches_df = matches_df.drop_duplicates(subset=['NotIn', 'InCou', 'SharedWordsStr'])

    # Keep rows where SharedWordsStr is unique (appears only once)
    unique_matches = matches_df[
        ~matches_df['SharedWordsStr'].duplicated(keep=False)
    ]
    unique_matches
    correction_map = dict(zip(unique_matches['NotIn'], unique_matches['InCou']))
    correction_map
    df['Country'] = df['Country'].apply(lambda x: correction_map.get(x, x))

    # -------------------------------- Final drop of unused columns
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
        "Last Update",
        "country_norm"
    ]

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")
    df.to_csv(Path(STAGING_DIR) / "Cleaned_Natural_Disasters.csv", index=False)

# ---------------------- DAG ----------------------

with DAG(
    dag_id="02_staging_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cleaning", "atomic"],
    schedule=None,  
    max_active_runs=1,
) as dag:

    create_staging = PythonOperator(
        task_id="create_staging_folder",
        python_callable=create_staging_dir
    )

    clean_bio = PythonOperator(
        task_id="clean_bio",
        python_callable=clean_bio_task
    )

    clean_res = PythonOperator(
        task_id="clean_res",
        python_callable=clean_res_task
    )
    clean_games = PythonOperator(
        task_id="clean_games",
        python_callable=clean_games_task
    )

    clean_cou = PythonOperator(
        task_id="clean_cou",
        python_callable=clean_cou_task
    )

    clean_disasters = PythonOperator(
        task_id="clean_natural_disasters", 
        python_callable=clean_natural_disasters)

    fix_permissions = BashOperator(
        task_id='fix_permissions',
        bash_command='chmod 644 /opt/airflow/data/staging/*.csv && chmod 755 /opt/airflow/data/staging/'
    )

    # All cleaning tasks can run in parallel after staging folder exists
    # The 'fix_permissions' task is needed to allow neo4j to access the created files
    create_staging >> clean_cou >> [clean_disasters, clean_bio, clean_res, clean_games] >> fix_permissions