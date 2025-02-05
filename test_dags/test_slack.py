from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="test_slack_notification",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    send_slack_message = SlackWebhookOperator(
        task_id="send_slack_message",
        http_conn_id="slack_connection",  # Uses the configured connection
        message="🚀 *Airflow Test:* Slack Webhook is working! ✅",
        username="airflow_bot",
    )

    send_slack_message
