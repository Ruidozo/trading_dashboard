import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ✅ Define connection ID
POSTGRES_CONN_ID = "project_postgres"

# ✅ Function to create yearly partitions
def create_yearly_partitions():
    year = datetime.utcnow().year  # Get the current year dynamically

    queries = [
        # ✅ Create partitions for stock_price_history
        f"""
        DO $$ 
        BEGIN 
            IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'stock_price_history_{year}') THEN
                EXECUTE 'CREATE TABLE stock_price_history_{year} 
                    PARTITION OF stock_price_history 
                    FOR VALUES FROM (''{year}-01-01'') TO (''{year+1}-01-01'');';
            END IF;
        END $$;
        """,

        # ✅ Create partitions for staging_company_news
        f"""
        DO $$ 
        BEGIN 
            IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'staging_company_news_{year}') THEN
                EXECUTE 'CREATE TABLE staging_company_news_{year} 
                    PARTITION OF staging_company_news 
                    FOR VALUES FROM (''{year}-01-01'') TO (''{year+1}-01-01'');';
            END IF;
        END $$;
        """
    ]

    # ✅ Execute queries
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    for query in queries:
        try:
            pg_hook.run(query)
            logging.info(f"✅ Created partitions for {year}")
        except Exception as e:
            logging.error(f"❌ Error creating partitions: {e}")

# ✅ Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),  # Run once a year starting 2025
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="create_partitions",
    default_args=default_args,
    schedule_interval="0 0 1 1 *",  # ✅ Run every January 1st
    catchup=False,
    tags=["postgres", "partitioning"],
) as dag:

    create_partitions_task = PythonOperator(
        task_id="create_partitions",
        python_callable=create_yearly_partitions,
    )

create_partitions_task  # ✅ Standalone DAG
