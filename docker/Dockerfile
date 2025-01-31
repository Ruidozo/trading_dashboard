FROM apache/airflow:2.6.3

# Switch to root to install system dependencies
USER root

# Install any additional system dependencies if needed (optional)
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

# Create necessary directories for data storage
RUN mkdir -p /opt/airflow/data/raw

# Ensure the airflow group exists before assigning ownership
RUN groupadd -f airflow && usermod -aG airflow airflow

# Set ownership back to the airflow user
RUN chown -R airflow:airflow /opt/airflow/data || true

# Switch back to airflow user
USER airflow

# Copy the requirements file to the container
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to airflow user
USER airflow

# Set the entrypoint to use bash
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Default command
CMD ["bash"]
