from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pytz

# --------------------------
# Slack Webhook URL (stored securely as Airflow Variable)
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
    dag_id = context.get("task_instance").dag_id
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
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")

    message = (
        f":large_green_circle: *Airflow Success Alert!* \n"
        f"*DAG:* {dag_id}\n"
        f"*Execution Time:* {execution_date}\n"
        f"*Status:* Success ✅\n"
    )

    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as e:
        print("Error sending success alert:", e)

# --------------------------
# Sample Python Tasks
# --------------------------
def task_1():
    print("Task 1 completed successfully!")

def task_2():
    print("Task 2 completed successfully!")

def task_3():
    # Intentionally fail this task to test failure alert
    raise Exception("Intentional failure in Task 3 to test Slack alert!")

# --------------------------
# Define DAG
# --------------------------
IST = pytz.timezone("Asia/Kolkata")

with DAG(
    dag_id="slack_success_failure_alert_dag",
    default_args=default_args,
    description="DAG with both success and failure Slack alerts",
    start_date=datetime(2025, 11, 6, tzinfo=IST),
    schedule_interval="40 14 * * *",  # Runs every day at 14:40 IST
    catchup=False,
    tags=["slack", "alert", "demo"],
) as dag:

    t1 = PythonOperator(
        task_id="task_1",
        python_callable=task_1,
        on_failure_callback=send_slack_failure_alert,
    )

    t2 = PythonOperator(
        task_id="task_2",
        python_callable=task_2,
        on_failure_callback=send_slack_failure_alert,
    )

    t3 = PythonOperator(
        task_id="task_3",
        python_callable=task_3,
        on_failure_callback=send_slack_failure_alert,
        on_success_callback=send_slack_success_alert,  # ✅ Success alert
    )

    t1 >> t2 >> t3
