# FastAPI serving layer, built FROM the Spark image on purpose:
# reading a Spark ML model from HDFS needs PySpark + the JVM + Hadoop client,
# all of which the apache/spark image already ships.
FROM apache/spark:3.5.7-python3

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The Spark distribution ships PySpark as zips, not as an installed package,
# so plain `python` cannot import it until PYTHONPATH points at them.
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# Driver memory is a JVM flag (-Xmx): it has to be an ENV var, setting
# spark.driver.memory inside SparkSession.builder is silently ignored.
ENV SPARK_DRIVER_MEMORY=1g
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app
COPY main.py /app/main.py

EXPOSE 8000
USER spark
CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
