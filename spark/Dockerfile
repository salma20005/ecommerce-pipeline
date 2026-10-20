FROM apache/spark:3.5.7-python3

# pyspark.ml (MLlib) needs numpy; the base image ships Python 3.8
# WITHOUT it, which crashes every ml_training.py run with
# "ModuleNotFoundError: No module named 'numpy'".
# numpy 1.24.4 is the newest release that still supports Python 3.8.
USER root
RUN pip install --no-cache-dir numpy==1.24.4
USER 185
