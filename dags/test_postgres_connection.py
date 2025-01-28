from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define the default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="test_postgres_connection",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to test the PostgreSQL connection
    test_connection = PostgresOperator(
        task_id="test_connection",
        postgres_conn_id="postgres_default",  # Ensure this matches the Airflow connection
        sql="SELECT 1;",  # Simple SQL query to test connection
    )
