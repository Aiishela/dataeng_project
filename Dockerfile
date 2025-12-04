FROM apache/airflow:3.1.0
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN pip install apache-airflow-providers-neo4j neo4j