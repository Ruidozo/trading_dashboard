FROM apache/airflow:2.6.3

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

RUN mkdir -p /opt/airflow/data/raw
RUN mkdir -p /opt/airflow/data

RUN groupadd -f airflow && usermod -aG airflow airflow
RUN chown -R airflow:airflow /opt/airflow/data
RUN chmod -R 775 /opt/airflow/data

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD ["bash"]