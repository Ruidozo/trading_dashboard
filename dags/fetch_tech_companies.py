import os
import finnhub
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def fetch_tech_companies():
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY is not set in the environment.")
    
    client = finnhub.Client(api_key=api_key)
    companies = client.stock_symbols('US')
    
    tech_companies = [company for company in companies if company['finnhubIndustry'] == 'Technology']
    return tech_companies

def store_tech_companies_in_postgres(**context):
    tech_companies = context['task_instance'].xcom_pull(task_ids='fetch_tech_companies_task')
    if not tech_companies:
        raise ValueError("No tech companies data fetched.")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tech_companies (
        symbol VARCHAR PRIMARY KEY,
        name VARCHAR,
        finnhubIndustry VARCHAR
    );
    """
    cursor.execute(create_table_query)
    
    for company in tech_companies:
        insert_query = """
        INSERT INTO tech_companies (symbol, name, finnhubIndustry)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE SET
        name = EXCLUDED.name,
        finnhubIndustry = EXCLUDED.finnhubIndustry;
        """
        cursor.execute(insert_query, (company['symbol'], company['description'], company['finnhubIndustry']))
    
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_tech_companies',
    default_args=default_args,
    description='Fetch top 500 tech companies and store in PostgreSQL',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    fetch_tech_companies_task = PythonOperator(
        task_id='fetch_tech_companies_task',
        python_callable=fetch_tech_companies,
    )
    
    store_tech_companies_task = PythonOperator(
        task_id='store_tech_companies_task',
        python_callable=store_tech_companies_in_postgres,
        provide_context=True,
    )
    
    fetch_tech_companies_task >> store_tech_companies_task
