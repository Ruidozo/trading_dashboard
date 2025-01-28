FROM apache/airflow:2.6.3

# Switch to root to install system dependencies
USER root

# Install any additional system dependencies if needed (optional)
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

# Switch back to airflow user
USER airflow

# Copy the requirements file to the container
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt