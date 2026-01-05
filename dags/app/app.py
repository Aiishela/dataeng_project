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

st.title("Correlation between natural disasters and olympic games")

st.markdown(
    """
    <style>
        .stApp {
        background: #7d4796;
        background-image: linear-gradient(to top, #6c4596, #563679, #41285d, #2d1b42, #1b0e29);
    </style>
    """,
    unsafe_allow_html=True
)
st.markdown(
    """
    Have you ever wondered if the natural disasters happening in a country 
    had an impact on the performance of said country during the Olympics?

    It is a rethorical question of course.

    You don't need to wait longer for the answers you so longed to have. 
    We combined those two informations and made some queries!
    """
)

# ---------------------------
# NEO4J CONNECTION
# ---------------------------
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "dataeng"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
analysis = st.selectbox(
    "Choose an analysis",
    [
        "Disasters before vs after hosting Olympics",
        "Natural disasters happening while winning a medal",
        "Country performance vs total natural disasters",
        "Performance before/after major earthquakes",
        "Top most resilient countries to natural disasters"
    ]
)

# ---------------------------
# CYPHER QUERIES
# ---------------------------
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
    d.subtype AS disaster_subtype,
    d.start_date AS start_date,
    d.location AS location,
    d.magnitude AS magnitude,
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

QUERY_DISASTERS_BEFORE_AFTER = """
CALL {
    // Disasters BEFORE the Olympics (within 5 years)
    MATCH (e:OlympicEdition)-[:HELD_IN]->(c:Country)
    MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
    WHERE d.start_date >= e.start_date - duration({years:5})
      AND d.start_date <  e.start_date
    RETURN
        e.start_date.year AS olympic_year,
        c.name AS country,
        COUNT(d) AS nb_before,
        0 AS nb_after

    UNION ALL

    // Disasters AFTER the Olympics (within 5 years)
    MATCH (e:OlympicEdition)-[:HELD_IN]->(c:Country)
    MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
    WHERE d.start_date >  e.end_date
      AND d.start_date <= e.end_date + duration({years:5})
    RETURN
        e.start_date.year AS olympic_year,
        c.name AS country,
        0 AS nb_before,
        COUNT(d) AS nb_after
}

RETURN
    olympic_year,
    country,
    SUM(nb_before) AS nb_disasters_before,
    SUM(nb_after)  AS nb_disasters_after
ORDER BY olympic_year DESC;

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

QUERY_RESILIENT_COUNTRIES = """
MATCH (c:Country)<-[:COMES_FROM]-(a:Athlete)-[:PARTICIPATED_IN]->(p)
WITH c,
     AVG(
         CASE p.medal
             WHEN 'Gold' THEN 3
             WHEN 'Silver' THEN 2
             WHEN 'Bronze' THEN 1
             ELSE 0
         END
     ) AS avg_score

OPTIONAL MATCH (d:NaturalDisaster)-[:AFFECTED]->(c)
WITH c, avg_score, COUNT(DISTINCT d) AS total_disasters
WHERE total_disasters > 0

RETURN c.name AS country,
       total_disasters,
       avg_score,
       avg_score / total_disasters AS resilience
ORDER BY resilience DESC
    """

# ---------------------------
# RUN QUERY BUTTON
# ---------------------------
if st.button("Run analysis"):
    with driver.session() as session:

        if analysis == "Disasters before vs after hosting Olympics":
            result = session.run(QUERY_DISASTERS_BEFORE_AFTER)
            query_explanation = """
                This query counts the number of natural disasters that occured in the country that hosted the
                olympics within a 5-year window (before and after) the games took place.
            """

        elif analysis == "Natural disasters happening while winning a medal":
            result = session.run(QUERY_MEDALS_DURING_DISASTERS)
            query_explanation = """
                This query counts how many natural disasters affected the country of origin of medal-winning
                athletes during the Olympic edition.
            """

        elif analysis == "Country performance vs total natural disasters":
            result = session.run(QUERY_COUNTRY_PERFORMANCE_VS_DISASTERS)
            query_explanation = """
                This query computes, per country:
            - an average medal score per participation (Gold = 3, Silver = 2, Bronze = 1, no medal = 0)
            - the total number of natural disasters that affected that country
            """

        elif analysis == "Performance before/after major earthquakes":
            result = session.run(QUERY_EVO_AFTER_EARTHQUAKES)
            query_explanation = """
                The query compares the average Olympic performance (Gold = 3, Silver = 2, Bronze = 1, no medal = 0) before and after major 
                earthquakes (magnitude ≥ 6) for countries affected by those earthquakes.
            """

        elif analysis == "Top most resilient countries to natural disasters":
            result = session.run(QUERY_RESILIENT_COUNTRIES)
            query_explanation = """
                This query ranks countries by a “resilience” metric defined as the average medal score divided
                by the number of disasters affecting that country.
            """

        rows = [record.data() for record in result]

    st.markdown(query_explanation)
    
    df = pd.DataFrame(rows)

    if df.empty:
        st.warning("No data returned from Neo4j.")
    else:
        st.subheader("Query Results")

        # ---------------------------
        # PLOTTING
        # ---------------------------
        if analysis == "Disasters before vs after hosting Olympics":
            agg = df.groupby("country", as_index=False)[
                ["nb_disasters_before", "nb_disasters_after"]
            ].sum()

            fig = px.bar(
                agg,
                x="country",
                y=["nb_disasters_before", "nb_disasters_after"],
                barmode="group",
                title="Number of natural disasters before and after hosting the Olympics for each country across all editions"
            )

        elif analysis == "Natural disasters happening while winning a medal":
            fig = px.bar(
                df,
                x="athlete",
                y="nb_disasters",
                color="medal",
                title="Natural disasters happening while winning a medal",
                hover_data=["country", "edition_year"]
            )

        elif analysis == "Country performance vs total natural disasters":
            fig = px.scatter(
                df,
                x="total_disasters",
                y="avg_score",
                hover_name="country",
                title="Country Performance vs Number of Natural Disasters",
                labels={"total_disasters": "Total Natural Disasters", "avg_score": "Average Olympic Score"}
            )

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

        elif analysis == "Top most resilient countries to natural disasters":
            fig = px.bar(
                df,
                x="country",
                y="resilience",
                color="resilience",
                title="Top Most Resilient Countries to Natural Disasters",
                labels={"resilience": "Average Score per Disaster"}
            )

        # st.dataframe(
        #     df.style.set_properties(**{"background-color": "white","color": "black"})
        # )
        st.dataframe(df)
        st.plotly_chart(fig, use_container_width=True)

driver.close()
