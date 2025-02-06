import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pandas as pd
import requests
import yfinance as yf

# Import functions from task scripts
from tasks.fetch_tech_companies import download_tech_companies_csv, process_tech_companies
from tasks.full_stock_pipeline import fetch_and_save_stock_data, upload_json_to_gcs, process_json_to_parquet, load_parquet_to_postgres, update_stock_price_history, detect_trading_patterns
from tasks.fetch_transform_news import fetch_company_news, transform_and_store_news
from tasks.analyze_news_stock_patterns import execute_sql, train_ml_model, predict_stock_movement

# Define Environment Variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")
POSTGRES_CONN_ID = "project_postgres"

# Slack Notification Function
def send_slack_notification(context, failed_tasks):
    if failed_tasks:
        message = f"❌ *Stock Pipeline Failed!* The following tasks failed: {', '.join(failed_tasks)} ⚠️"
    else:
        message = "✅ *Stock Pipeline Completed Successfully!* 📊"
    
    slack_alert = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id="slack_connection",
        message=message,
        username="airflow_bot",
    )
    return slack_alert.execute(context=context)

def slack_callback(**context):
    failed_tasks = [
        t.task_id for t in context['dag_run'].get_task_instances() if t.state == 'failed'
    ]
    return send_slack_notification(context, failed_tasks)

# DAG Definition
with DAG(
    dag_id="trading_dashboard",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2025, 1, 1),
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 2 * * *",  # Runs daily at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["trading_dashboard", "postgres", "gcs"],
    on_failure_callback=slack_callback,
    on_success_callback=slack_callback,
) as dag:

    with TaskGroup("trading_dashboard") as trading_dashboard:
        # Fetch & Process Tech Companies
        download_task = PythonOperator(task_id="download_csv", python_callable=download_tech_companies_csv)
        process_task = PythonOperator(task_id="process_tech_companies", python_callable=process_tech_companies)

        # Fetch & Process Stock Data
        fetch_stock_task = PythonOperator(task_id="fetch_stock_data", python_callable=fetch_and_save_stock_data)
        upload_json_task = PythonOperator(task_id="upload_json_to_gcs", python_callable=upload_json_to_gcs)
        process_parquet_task = PythonOperator(task_id="process_json_to_parquet", python_callable=process_json_to_parquet)
        load_postgres_task = PythonOperator(task_id="load_parquet_to_postgres", python_callable=load_parquet_to_postgres)
        update_stock_task = PythonOperator(task_id="update_stock_price_history", python_callable=update_stock_price_history)
        detect_patterns_task = PythonOperator(task_id="detect_trading_patterns", python_callable=detect_trading_patterns)

        # Fetch & Transform News
        fetch_news_task = PythonOperator(task_id="fetch_news", python_callable=fetch_company_news)
        transform_news_task = PythonOperator(task_id="transform_news", python_callable=transform_and_store_news)

        # Analyze News & Stock Patterns
        run_analysis = PythonOperator(task_id="run_news_stock_analysis", python_callable=execute_sql)
        train_model = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model)
        predict_movement = PythonOperator(task_id="predict_stock_movement", python_callable=predict_stock_movement)

        # Slack Notifications
        slack_task = PythonOperator(task_id="send_slack_notification", python_callable=slack_callback, provide_context=True)

        # Define Dependencies
        download_task >> process_task >> fetch_stock_task
        fetch_stock_task >> upload_json_task >> process_parquet_task >> load_postgres_task >> update_stock_task >> detect_patterns_task
        fetch_news_task >> transform_news_task
        [detect_patterns_task, transform_news_task] >> run_analysis >> train_model >> predict_movement
        predict_movement >> slack_task
