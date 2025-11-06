from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -----------------------------
# Define Python callables
# -----------------------------

def task_1(**context):
    msg = "Data received from source A"
    print(msg)
    # Push value to XCom
    context['ti'].xcom_push(key='source_data', value=msg)
    return msg

def task_2(**context):
    msg = context['ti'].xcom_pull(key='source_data', task_ids='task_1')
    print("Task 2 got:", msg)
    new_msg = msg + " → transformed in task_2"
    context['ti'].xcom_push(key='transformed_data', value=new_msg)

def task_3(**context):
    msg = context['ti'].xcom_pull(key='transformed_data', task_ids='task_2')
    print("Task 3 got:", msg)
    final_msg = msg + " → enriched in task_3"
    context['ti'].xcom_push(key='final_data', value=final_msg)

def task_4(**context):
    msg = context['ti'].xcom_pull(key='final_data', task_ids='task_3')
    print("Task 4 received:", msg)
    print("✅ Task 4 completed processing")

def pyspark_parquet_to_csv(**context):
    print("Starting PySpark task...")
    spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()

    input_path = "/mnt/c/data/pyspark_transform.parquet"
    output_path = "/mnt/d/ocheyi"


    df = spark.read.parquet(input_path)
    print("Initial Data:")
    df.show(5)

    df_clean = df.dropna()
    print("Rows after cleaning:", df_clean.count())
    df_clean.describe().show()

    df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"✅ CSV written to {output_path}")

    spark.stop()

# -----------------------------
# DAG definition
# -----------------------------
default_args = {'owner': 'airflow', 'start_date': datetime(2025, 11, 1)}

with DAG(
    dag_id='five_task_dag_with_xcom_pyspark',
    default_args=default_args,
    description='DAG with 5 tasks using XCom and PySpark job',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id='task_1', python_callable=task_1, provide_context=True)
    t2 = PythonOperator(task_id='task_2', python_callable=task_2, provide_context=True)
    t3 = PythonOperator(task_id='task_3', python_callable=task_3, provide_context=True)
    t4 = PythonOperator(task_id='task_4', python_callable=task_4, provide_context=True)
    t5 = PythonOperator(task_id='pyspark_parquet_to_csv', python_callable=pyspark_parquet_to_csv)

    # Define dependencies
    t1 >> t2 >> t3 >> t4 >> t5
