from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print("Hello from GitHub DAG!")

with DAG(
    dag_id="api_to_adls_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    greet_task = PythonOperator(
        task_id="greet_task",
        python_callable=greet
    )
