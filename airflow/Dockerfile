FROM apache/airflow:2.8.1-python3.10

USER root

# System dependencies
RUN apt-get update && apt-get install -y python3-pip curl && rm -rf /var/lib/apt/lists/*

# Python deps needed for DAGs / SparkSubmitOperator
RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    delta-spark==3.2.0 \
    psycopg2-binary \
    neo4j

# Switch back to airflow user
USER airflow

# Working directory
WORKDIR /opt/airflow
