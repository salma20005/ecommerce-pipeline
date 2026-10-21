# Airflow image for the batch-orchestration stage of the poster.
# Slim base + the docker CLI: DAG tasks run as
#     docker exec spark /opt/spark/bin/spark-submit ...
# i.e. Airflow orchestrates, the containers we already have do the work.
# (The scheduler mounts /var/run/docker.sock and joins group 0,
#  because the socket is srw-rw---- root:root.)
FROM apache/airflow:slim-2.10.5

USER root
# The laptop runs on a slow/unstable connection: apt and pip both get
# generous retries + timeouts so one dropped packet does not kill the build.
RUN apt-get update \
    -o Acquire::Retries=10 \
    -o Acquire::http::Timeout=120 \
    && apt-get install -y --no-install-recommends \
       -o Dpkg::Options::="--force-confold" docker.io curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow
# the slim image ships WITHOUT the postgres driver Airflow needs to talk
# to the airflow-db metadata database (SQL_ALCHEMY_CONN=postgresql+psycopg2).
# NOTE: must run as 'airflow' -- the official image REFUSES pip as root.
ENV PIP_DEFAULT_TIMEOUT=180
RUN pip install --no-cache-dir --retries 10 --timeout 60 psycopg2-binary
