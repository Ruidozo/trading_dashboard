import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# Define the path to the local JSON files
LOCAL_JSON_PATH = "/opt/airflow/data/raw/"

# Get GCS bucket name from environment
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
if not GCS_BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME is not set in the environment.")

 # Upload Function
def upload_to_gcs():
    """Upload local JSON files to Google Cloud Storage and delete them locally."""
    gcs_hook = GCSHook()

    for root, _, files in os.walk(LOCAL_JSON_PATH):
        for file in files:
            if file.endswith(".json"):
                local_file_path = os.path.join(root, file)
                gcs_file_path = os.path.relpath(local_file_path, LOCAL_JSON_PATH)

                try:
                    gcs_hook.upload(
                        bucket_name=GCS_BUCKET_NAME,
                        object_name=gcs_file_path,
                        filename=local_file_path
                    )
                    logging.info(f"✅ Successfully uploaded {local_file_path} to gs://{GCS_BUCKET_NAME}/{gcs_file_path}")

                    # Delete the local file after successful upload
                    os.remove(local_file_path)
                    logging.info(f"🗑️ Deleted local file: {local_file_path}")

                except Exception as e:
                    logging.error(f"❌ Failed to upload {local_file_path} to GCS: {e}")
                    raise  # Ensure failure triggers Slack alert

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="upload_to_gcs",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # Run daily at 1 AM UTC
    catchup=False,
    tags=["gcs", "data_upload"],
) as dag:
    
    upload_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
    )

    upload_task
