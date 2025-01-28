from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Define the test function
def print_hello():
    print("Hello from Airflow! DAG imported successfully.")

# Define the DAG
with DAG(
    dag_id="test_import",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to test the DAG
    task_hello = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )
