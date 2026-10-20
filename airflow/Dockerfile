# Airflow image for the batch-orchestration stage of the poster.
# Slim base + the docker CLI: DAG tasks run as
#     docker exec spark /opt/spark/bin/spark-submit ...
# i.e. Airflow orchestrates, the containers we already have do the work.
# (The scheduler mounts /var/run/docker.sock and joins group 0,
#  because the socket is srw-rw---- root:root.)
FROM apache/airflow:slim-2.10.5

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow
# the slim image ships WITHOUT the postgres driver Airflow needs to talk
# to the airflow-db metadata database (SQL_ALCHEMY_CONN=postgresql+psycopg2).
# NOTE: must run as 'airflow' -- the official image REFUSES pip as root.
RUN pip install --no-cache-dir psycopg2-binary
