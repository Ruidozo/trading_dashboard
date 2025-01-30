import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import finnhub
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dag_config import POSTGRES_CONN_ID

# Set up Airflow logger
log = logging.getLogger(__name__)

def fetch_tech_companies():
    """Fetch top tech companies from Finnhub and store in PostgreSQL."""
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        log.error("❌ FINNHUB_API_KEY is not set in the environment.")
        raise ValueError("❌ FINNHUB_API_KEY is not set in the environment.")

    client = finnhub.Client(api_key=api_key)
    companies = client.stock_symbols('US')

    # Log only the count, not full data
    log.info("✅ Fetched %d total stocks from Finnhub API.", len(companies))

    if not companies:
        log.error("❌ Finnhub API returned no stock symbols!")
        raise ValueError("❌ Finnhub API returned no stock symbols!")

    # Filter only tech companies
    tech_companies = [
        company for company in companies
        if company.get('description') and any(word in company['description'].lower() for word in ['tech', 'software', 'semiconductor', 'computer', 'digital'])
    ]

    # Log summary instead of full data
    log.info("✅ Found %d tech companies.", len(tech_companies))
    if tech_companies:
        log.info("Example tech company: %s", tech_companies[0])  # Show only one

    return tech_companies

# Airflow DAG
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
    schedule='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    fetch_tech_companies_task = PythonOperator(
        task_id='fetch_tech_companies_task',
        python_callable=fetch_tech_companies,
    )



def store_tech_companies_in_postgres(**context):
    tech_companies = context['task_instance'].xcom_pull(task_ids='fetch_tech_companies_task')
    
    if not tech_companies:
        log.warning("⚠️ No tech companies fetched. Skipping storage task.")
        return

    log.info(f"📌 Storing {len(tech_companies)} tech companies in PostgreSQL.")

    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Ensure table exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tech_companies (
            symbol VARCHAR PRIMARY KEY,
            name VARCHAR,
            finnhubIndustry VARCHAR
        );
        """
        cursor.execute(create_table_query)

        for company in tech_companies:
            log.info(f"📝 Inserting company: {company.get('symbol', '')} - {company.get('description', '')}")
            cursor.execute("""
                INSERT INTO tech_companies (symbol, name, finnhubIndustry)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                finnhubIndustry = EXCLUDED.finnhubIndustry;
            """, (company.get('symbol', ''), company.get('description', ''), company.get('finnhubIndustry', '')))

        conn.commit()
        cursor.close()
        conn.close()

        log.info("✅ Successfully inserted tech companies into PostgreSQL.")
    except Exception as e:
        log.error(f"❌ Error inserting data into PostgreSQL: {e}")
        raise

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
    schedule='@monthly',
    start_date=datetime(2023, 1, 1),
    tags=["finnhub", "fetch companies", "postgres"],
    catchup=False,
) as dag:
    
    fetch_tech_companies_task = PythonOperator(
        task_id='fetch_tech_companies_task',
        python_callable=fetch_tech_companies,
    )
    
    store_tech_companies_task = PythonOperator(
        task_id='store_tech_companies_task',
        python_callable=store_tech_companies_in_postgres,
    )
    
    fetch_tech_companies_task >> store_tech_companies_task