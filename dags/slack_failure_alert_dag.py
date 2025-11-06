from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
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
# Slack alert function
# --------------------------
def send_slack_alert(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = (
        f":red_circle: *Airflow Alert!* \n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Time:* {execution_date}\n"
        f"*Status:* Failed ❌\n"
        f"<{log_url}|View Logs>"
    )

    payload = {"text": message}
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print("Error sending Slack alert:", e)

# --------------------------
# Python functions for tasks
# --------------------------
def task_1():
    print("Task 1 completed successfully!")

def task_2():
    print("Task 2 completed successfully!")

def task_3():
    raise Exception("Intentional failure in Task 3 to test Slack alert!")

# --------------------------
# Define the DAG
# Runs every day at 14:40 IST = 09:10 UTC  --> cron: "10 9 * * *"
# --------------------------
with DAG(
    dag_id="slack_failure_alert_dag",
    default_args=default_args,
    description="DAG that fails at task 3 and sends Slack alert",
    start_date=datetime(2025, 11, 1),          # keep in the past; Airflow schedules in UTC
    schedule_interval="10 9 * * *",            # 09:10 UTC = 14:40 IST
    catchup=False,
    tags=["slack", "alert", "demo"],
) as dag:

    t1 = PythonOperator(
        task_id="task_1",
        python_callable=task_1,
        on_failure_callback=send_slack_alert,
    )

    t2 = PythonOperator(
        task_id="task_2",
        python_callable=task_2,
        on_failure_callback=send_slack_alert,
    )

    t3 = PythonOperator(
        task_id="task_3",
        python_callable=task_3,
        on_failure_callback=send_slack_alert,
    )

    t1 >> t2 >> t3
