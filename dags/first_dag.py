from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 11, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 11, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )
