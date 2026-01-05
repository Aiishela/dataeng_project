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
# ---------------------- NEO4J ----------------------
def create_country_constraint():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT country_noc IF NOT EXISTS
            FOR (c:Country) REQUIRE c.noc IS UNIQUE;

        """)
def create_edition_constraint():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT edition_id IF NOT EXISTS
            FOR (e:OlympicEdition) REQUIRE e.edition_id IS UNIQUE;

        """)

def create_event_constraint():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT sport_event IF NOT EXISTS
            FOR (s:SportEvent) REQUIRE (s.sport, s.event) IS UNIQUE;

        """)

def create_disaster_constraint():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT disaster_id IF NOT EXISTS
            FOR (d:NaturalDisaster) REQUIRE d.dis_no IS UNIQUE;

        """)
def create_athlete_constraint():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT athlete_id IF NOT EXISTS
            FOR (a:Athlete) REQUIRE a.athlete_id IS UNIQUE;

        """)


def create_country_nodes():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///Olympics_Country.csv' AS row
            MERGE (c:Country {noc: row.noc})
            SET c.name = row.country;
        """)


def create_athlete_nodes():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CALL apoc.periodic.iterate(
            "LOAD CSV WITH HEADERS FROM 'file:///Olympic_Athlete_Bio.csv' AS row RETURN row",
            "
            MERGE (a:Athlete {athlete_id: row.athlete_id})
            SET a.name = row.name,
                a.gender = row.sex

            WITH row, a
            MATCH (c:Country {noc: row.country_noc})
            MERGE (a)-[:COMES_FROM]->(c)
            ",
            {batchSize: 500, parallel: false}
            )

        """)

def create_edition_nodes():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///Olympics_Games.csv' AS row
            MERGE (e:OlympicEdition {edition_id: toInteger(row.edition_id)})
            SET e.start_date = date(row.competition_start_date),
                e.end_date = date(row.competition_end_date)
            WITH row, e
            MATCH (c:Country {noc: row.country_noc})
            MERGE (e)-[:HELD_IN]->(c);

        """)

def create_event_nodes():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///Olympic_Athlete_Event_Results.csv' AS row
            MERGE (s:SportEvent {
            sport: row.sport,
            event: row.event
            });
        """)

def create_participation_nodes():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CALL apoc.periodic.iterate(
            "LOAD CSV WITH HEADERS FROM 'file:///Olympic_Athlete_Event_Results.csv' AS row RETURN row",
            "
            CREATE (p:Participation)
            SET p.medal = CASE
                WHEN row.medal IS NULL OR row.medal = '' THEN NULL
                ELSE row.medal
            END

            WITH row, p
            MATCH (a:Athlete {athlete_id: row.athlete_id})
            MATCH (e:OlympicEdition {edition_id: toInteger(row.edition_id)})
            MATCH (s:SportEvent {sport: row.sport, event: row.event})
            MATCH (c:Country {noc: row.country_noc})

            MERGE (a)-[:PARTICIPATED_IN]->(p)
            MERGE (p)-[:IN_EDITION]->(e)
            MERGE (p)-[:IN_EVENT]->(s)
            MERGE (p)-[:REPRESENTED]->(c)
            ",
            {batchSize: 500, parallel: false}
            )


        """)

def create_disaster_nodes():
    from neo4j import GraphDatabase

    uri = "bolt://neo:7687"   # service name from docker-compose
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        session.run("""
            CALL apoc.periodic.iterate(
            "LOAD CSV WITH HEADERS FROM 'file:///02_normalize_date.csv' AS row RETURN row",
            "
            MERGE (d:NaturalDisaster {dis_no: row.`DisNo.`})
            SET d.group = row.`Disaster Group`,
                d.subgroup = row.`Disaster Subgroup`,
                d.type = row.`Disaster Type`,
                d.subtype = row.`Disaster Subtype`,
                d.magnitude = row.Magnitude,
                d.location = row.Location,
                d.start_date = date(row.`Start Date`),
                d.end_date = date(row.`End Date`)

            WITH d, row
            MATCH (c:Country {name: row.Country})
            MERGE (d)-[:AFFECTED]->(c)
            WITH d     
            MATCH (e:OlympicEdition)
            WHERE e.start_date IS NOT NULL AND e.end_date IS NOT NULL

            FOREACH (_ IN CASE WHEN d.start_date <= e.end_date AND d.end_date >= e.start_date THEN [1] ELSE [] END |
                MERGE (d)-[:OVERLAP_EDITION]->(e)
            )
            FOREACH (_ IN CASE WHEN d.end_date < e.start_date THEN [1] ELSE [] END |
                MERGE (d)-[:PRECEDES_EDITION]->(e)
            )
            FOREACH (_ IN CASE WHEN d.start_date > e.end_date THEN [1] ELSE [] END |
                MERGE (d)-[:AFTER_EDITION]->(e)
            )
            ",
            {batchSize: 500, parallel: false}
            )



        """)
# ---------- TASKS: cleaning functions (imports inside to speed up DAG import) ----------



# ---------------------- DAG ----------------------

with DAG(
    dag_id="test_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "cypher"],
    schedule=None,  # run manually or trigger
) as dag:


    create_country_constraint_task = PythonOperator(
        task_id="create_country_constraint",
        python_callable=create_country_constraint
    )
    create_athlete_constraint_task = PythonOperator(
        task_id="create_athlete_constraint",
        python_callable=create_athlete_constraint
    )
    create_edition_constraint_task = PythonOperator(
        task_id="create_edition_constraint",
        python_callable=create_edition_constraint
    )
    create_event_constraint_task = PythonOperator(
        task_id="create_event_constraint",
        python_callable=create_event_constraint
    )
    create_disaster_constraint_task = PythonOperator(
        task_id="create_disaster_constraint",
        python_callable=create_disaster_constraint
    )


    create_country_nodes_task = PythonOperator(
        task_id="create_country_nodes",
        python_callable=create_country_nodes
    )
    create_athlete_nodes_task = PythonOperator(
        task_id="create_athlete_nodes",
        python_callable=create_athlete_nodes
    )
    create_edition_nodes_task = PythonOperator(
        task_id="create_edition_nodes",
        python_callable=create_edition_nodes
    )
    create_event_nodes_task = PythonOperator(
        task_id="create_event_nodes",
        python_callable=create_event_nodes
    )
    create_participation_nodes_task = PythonOperator(
        task_id="create_participation_nodes",
        python_callable=create_participation_nodes
    )
    create_disaster_nodes_task = PythonOperator(
        task_id="create_disaster_nodes",
        python_callable=create_disaster_nodes
    )

    # ---- Constraints (all parallel) ----
    constraints = [
        create_country_constraint_task,
        create_athlete_constraint_task,
        create_edition_constraint_task,
        create_event_constraint_task,
        create_disaster_constraint_task,
    ]

    # ---- Countries ----
    create_country_constraint_task >> create_country_nodes_task

    # ---- Athletes, Editions, Events (parallel) ----
    [create_athlete_constraint_task, create_country_nodes_task] >> create_athlete_nodes_task
    [create_edition_constraint_task, create_country_nodes_task] >> create_edition_nodes_task
    create_event_constraint_task >> create_event_nodes_task

    # ---- Participation ----
    [
        create_athlete_nodes_task,
        create_edition_nodes_task,
        create_event_nodes_task,
        create_country_nodes_task,
    ] >> create_participation_nodes_task

    # ---- Disasters ----
    [
        create_disaster_constraint_task,
        create_country_nodes_task,
        create_edition_nodes_task,
    ] >> create_disaster_nodes_task
