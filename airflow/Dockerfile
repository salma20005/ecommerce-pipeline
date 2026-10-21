# Airflow image for the batch-orchestration stage of the poster.
# DAG tasks run as
#     docker exec spark /opt/spark/bin/spark-submit ...
# i.e. Airflow orchestrates, the containers we already have do the work.
# (The scheduler mounts /var/run/docker.sock and joins group 0,
#  because the socket is srw-rw---- root:root.)
#
# DESIGN: multi-stage, NO apt-get.
# `apt-get install docker.io` pulls ~100 MB of debs from Debian mirrors,
# and this laptop's connection keeps dropping mid-download (the whole
# image build died twice on it). The docker CLI is a single static
# binary, so we copy it out of the official docker:27-cli image instead:
# one layer, one pull, no mirrors, no dpkg.
FROM docker:27-cli AS dockercli

FROM apache/airflow:slim-2.10.5

USER root
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
USER airflow

# the slim image ships WITHOUT the postgres driver Airflow needs to talk
# to the airflow-db metadata database (SQL_ALCHEMY_CONN=postgresql+psycopg2).
# NOTE: must run as 'airflow' -- the official image REFUSES pip as root.
# The line is slow here too, so give pip a long timeout and many retries.
ENV PIP_DEFAULT_TIMEOUT=180
RUN pip install --no-cache-dir --retries 10 --timeout 60 psycopg2-binary

# No curl in this image on purpose: the compose healthcheck calls python3,
# which the base image already has.
