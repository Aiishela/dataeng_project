import streamlit as st
import pandas as pd
from neo4j import GraphDatabase
import plotly.express as px

# ---------------------------
# PAGE CONFIG
# ---------------------------
st.set_page_config(
    page_title="Olympics & Disasters",
    layout="wide"
)

st.title("🏟️ Natural Disasters Before & After Hosting Olympics")

st.markdown(
    """
    This dashboard explores whether countries experience **more natural disasters**
    in the **5 years before or after hosting the Olympic Games**.
    """
)

# ---------------------------
# NEO4J CONNECTION
# ---------------------------
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "dataeng"  # ⬅️ change this

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
analysis = st.selectbox(
    "Choose an analysis",
    [
        "Disasters before vs after hosting Olympics",
        "Medals won during natural disasters",
        "Country performance vs total natural disasters",
        "Top most resilient countries to natural disasters",
        "Performance before/after major earthquakes"
    ]
)
QUERY_RESILIENT_COUNTRIES = """
        MATCH (a:Athlete)-[:COMES_FROM]->(c:Country)
        OPTIONAL MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
        MATCH (a)-[:PARTICIPATED_IN]->(p)
        WITH c, COUNT(d) AS total_disasters,
        AVG(
        CASE p.medal
            WHEN 'Gold' THEN 3
            WHEN 'Silver' THEN 2
            WHEN 'Bronze' THEN 1
            ELSE 0
        END
        ) AS avg_score
        WHERE total_disasters > 0
        RETURN c.name AS country,
            total_disasters,
            avg_score,
            avg_score / total_disasters AS resilience
        ORDER BY resilience DESC
    """

QUERY_EVO_AFTER_EARTHQUAKES = """
    MATCH (d:NaturalDisaster)-[:AFFECTED]->(c:Country)
    WHERE d.magnitude >= 6
    AND d.type = "Earthquake"
    MATCH (a:Athlete)-[:PARTICIPATED_IN]->(p:Participation)-[:IN_EDITION]->(e)
    MATCH (p)-[:REPRESENTED]->(c)
    WITH c, d, e,
        CASE p.medal
        WHEN 'Gold' THEN 3
        WHEN 'Silver' THEN 2
        WHEN 'Bronze' THEN 1
        ELSE 0
        END AS score
    WITH c, d,
        AVG(CASE WHEN e.end_date < d.start_date THEN score END) AS avg_before,
        AVG(CASE WHEN e.start_date > d.end_date THEN score END) AS avg_after
    RETURN c.name AS country,
        d.dis_no AS disaster_id,
        d.subtype AS disaster_subtype,
        avg_before,
        avg_after
"""

QUERY_COUNTRY_PERFORMANCE_VS_DISASTERS = """
MATCH (a:Athlete)-[:PARTICIPATED_IN]->(p:Participation)-[:REPRESENTED]->(c:Country)
WITH c,
     CASE p.medal
       WHEN 'Gold' THEN 3
       WHEN 'Silver' THEN 2
       WHEN 'Bronze' THEN 1
       ELSE 0
     END AS score
WITH c, COLLECT(score) AS scores
WITH c, REDUCE(s=0, x IN scores | s + x) * 1.0 / SIZE(scores) AS avg_score

OPTIONAL MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
WITH c, avg_score, COUNT(d) AS total_disasters

RETURN c.name AS country,
       avg_score,
       total_disasters
ORDER BY total_disasters DESC, avg_score DESC
"""

# ---------------------------
# CYPHER QUERY
# ---------------------------
QUERY_DISASTERS_BEFORE_AFTER = """
MATCH (e:OlympicEdition)-[:HELD_IN]->(c:Country)
MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
WHERE e.start_date >= d.end_date
AND d.start_date <= e.end_date + duration({years:5})
RETURN c.name AS country,
       e.start_date.year AS olympic_year,
       d.type AS disaster_type,
       COUNT(d) AS nb_disasters_before,
       0 AS nb_disasters_after

UNION

MATCH (e:OlympicEdition)-[:HELD_IN]->(c:Country)
MATCH (nd:NaturalDisaster)-[:AFFECTED]->(c)
WHERE nd.start_date >= e.end_date
AND e.start_date <= nd.end_date + duration({years:5})
RETURN c.name AS country,
       e.start_date.year AS olympic_year,
       nd.type AS disaster_type,
       0 AS nb_disasters_before,
       COUNT(nd) AS nb_disasters_after
"""
QUERY_MEDALS_DURING_DISASTERS = """
MATCH (a:Athlete)-[:PARTICIPATED_IN]->(p:Participation)-[:IN_EDITION]->(e:OlympicEdition),
      (a)-[:COMES_FROM]->(c:Country)
MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
WHERE d.start_date <= e.end_date
  AND d.end_date >= e.start_date
  AND p.medal IS NOT NULL
RETURN a.name AS athlete,
       c.name AS country,
       e.start_date.year AS edition_year,
       p.medal AS medal,
       COUNT(DISTINCT d) AS nb_disasters
ORDER BY nb_disasters DESC
"""


# ---------------------------
# RUN QUERY BUTTON
# ---------------------------
if st.button("Run analysis"):
    with driver.session() as session:

        if analysis == "Disasters before vs after hosting Olympics":
            result = session.run(QUERY_DISASTERS_BEFORE_AFTER)

        elif analysis == "Medals won during natural disasters":
            result = session.run(QUERY_MEDALS_DURING_DISASTERS)

        elif analysis == "Country performance vs total natural disasters":
            result = session.run(QUERY_COUNTRY_PERFORMANCE_VS_DISASTERS)

        elif analysis == "Top most resilient countries to natural disasters":
            result = session.run(QUERY_RESILIENT_COUNTRIES)

        elif analysis == "Performance before/after major earthquakes":
            result = session.run(QUERY_EVO_AFTER_EARTHQUAKES)

        rows = [record.data() for record in result]

    df = pd.DataFrame(rows)

    if df.empty:
        st.warning("No data returned from Neo4j.")
    else:
        st.subheader("📋 Query Results")
        st.dataframe(df)

        # ---------------------------
        # PLOTTING
        # ---------------------------
        if analysis == "Disasters before vs after hosting Olympics":
            agg = df.groupby(["country", "olympic_year"]).agg({
                "nb_disasters_before": "sum",
                "nb_disasters_after": "sum"
            }).reset_index()

            fig = px.bar(
                agg,
                x="country",
                y=["nb_disasters_before", "nb_disasters_after"],
                barmode="group",
                title="Natural Disasters Before vs After Hosting Olympics"
            )
            st.plotly_chart(fig, use_container_width=True)

        elif analysis == "Medals won during natural disasters":
            fig = px.bar(
                df,
                x="athlete",
                y="nb_disasters",
                color="medal",
                title="Medals Won While Natural Disasters Were Occurring",
                hover_data=["country", "edition_year"]
            )
            st.plotly_chart(fig, use_container_width=True)

        elif analysis == "Country performance vs total natural disasters":
            fig = px.scatter(
                df,
                x="total_disasters",
                y="avg_score",
                hover_name="country",
                title="Country Performance vs Number of Natural Disasters",
                labels={"total_disasters": "Total Natural Disasters", "avg_score": "Average Olympic Score"}
            )
            st.plotly_chart(fig, use_container_width=True)

        elif analysis == "Top most resilient countries to natural disasters":
            fig = px.bar(
                df,
                x="country",
                y="resilience",
                color="resilience",
                title="Top Most Resilient Countries to Natural Disasters",
                labels={"resilience": "Average Score per Disaster"}
            )
            st.plotly_chart(fig, use_container_width=True)

        elif analysis == "Performance before/after major earthquakes":
            fig = px.scatter(
                df,
                x="avg_before",
                y="avg_after",
                hover_name="country",
                color="disaster_subtype",
                title="Average Country Performance Before vs After Major Earthquakes",
                labels={"avg_before": "Average Score Before", "avg_after": "Average Score After"}
            )
            st.plotly_chart(fig, use_container_width=True)




driver.close()
