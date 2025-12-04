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
    "retry_delay": timedelta(seconds=30)
}
# ---------------------- NEO4J ----------------------

def run_query():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            // Unique constraint on NaturalDisaster.DisNo
            CREATE CONSTRAINT natural_disaster_disno IF NOT EXISTS
            FOR (d:NaturalDisaster)
            REQUIRE d.DisNo IS UNIQUE;
        """)
        session.run("""
            // Unique constraint on Country.name
            CREATE CONSTRAINT country_name IF NOT EXISTS
            FOR (c:Country)
            REQUIRE c.name IS UNIQUE;
        """)
        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///02_normalize_date.csv" AS row
            WITH DISTINCT row.Country AS country,
                            row.ISO AS iso,
                            row.NOC AS noc
            WHERE country IS NOT NULL AND country <> ""

            MERGE (c:Country {name: country})
            SET c.iso = iso,
                c.noc = noc;
        """)
        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///02_normalize_date.csv" AS row

            WITH row
            WHERE row.`DisNo.` IS NOT NULL AND row.`DisNo.` <> ""

            MERGE (d:NaturalDisaster {DisNo: row.`DisNo.`})
            SET d.group       = row.`Disaster Group`,
                d.subgroup    = row.`Disaster Subgroup`,
                d.type        = row.`Disaster Type`,
                d.subtype     = row.`Disaster Subtype`,
                d.event_name  = row.`Event Name`,
                d.start_date  = row.`Start Date`,
                d.end_date    = row.`End Date`,
                d.latitude    = row.`Latitude`,
                d.longitude   = row.`Longitude`

            WITH d, row
            MATCH (c:Country {name: row.Country})
            MERGE (d)-[:HAPPENED_IN]->(c);
        """)
        

# ---------------------- DAG ----------------------

with DAG(
    dag_id="production_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["production", "atomic"],
    schedule=None,  # run manually or trigger
) as dag:

    run_cypher_disaster = PythonOperator(
        task_id="neo4j_query",
        python_callable=run_query
    )

    run_cypher_disaster
