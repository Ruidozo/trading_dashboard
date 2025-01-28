FROM apache/airflow:2.6.3

# Install dependencies
USER root
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && apt-get clean

# Switch back to airflow user
USER airflow
