from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

# --------------------------
# Slack Webhook URL (from Airflow Variable)
# --------------------------
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url")

# --------------------------
# Default DAG arguments
# --------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# --------------------------
# Slack alert for FAILURE
# --------------------------
def send_slack_failure_alert(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = (
        f":red_circle: *Airflow Failure Alert!* \n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Time:* {execution_date}\n"
        f"*Status:* Failed ❌\n"
        f"<{log_url}|View Logs>"
    )

    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as e:
        print("Error sending failure alert:", e)


# --------------------------
# Slack alert for SUCCESS
# --------------------------
def send_slack_success_alert(context):
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    message = (
        f":large_green_circle: *Airflow Success Alert!* \n"
        f"*DAG:* {dag_id}\n"
        f"*Execution Time:* {execution_date}\n"
        f"*Status:* Success ✅\n"
    )
