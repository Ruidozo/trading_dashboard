import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

def create_next_year_partition():
    """Automatically create the next year's partition for stock_price_history."""
    next_year = datetime.now().year + 1  # Get upcoming year
    partition_name = f"stock_price_history_{next_year}"
    
    sql_check = f"""
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = '{partition_name}'
    );
    """
    
    sql_create = f"""
    CREATE TABLE {partition_name} PARTITION OF stock_price_history
    FOR VALUES FROM ('{next_year}-01-01') TO ('{next_year + 1}-01-01');
    """
    
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    result = pg_hook.get_first(sql_check)
    
    if not result[0]:  # If partition doesn't exist, create it
        log.info(f"📌 Creating new partition: {partition_name}")
        pg_hook.run(sql_create)
        log.info(f"✅ Partition {partition_name} created successfully!")
    else:
        log.info(f"✅ Partition {partition_name} already exists. No action needed.")

# Airflow DAG Setup
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "create_stock_partitions",
    default_args=default_args,
    description="Automatically create a new partition for stock_price_history",
    schedule_interval="0 0 1 12 *",  # Runs every December 1st
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stock-data", "postgres", "partitioning"],
) as dag:

    create_partition_task = PythonOperator(
        task_id="create_partition",
        python_callable=create_next_year_partition,
    )

    create_partition_task
