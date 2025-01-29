from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import requests

def test_dependencies():
    # Test pandas
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    print(df)

    # Test requests
    response = requests.get("https://jsonplaceholder.typicode.com/posts/1")
    print(response.json())

with DAG(
    dag_id="test_dependencies",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    tags=["test"],
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id="test_dependencies",
        python_callable=test_dependencies,
    )
