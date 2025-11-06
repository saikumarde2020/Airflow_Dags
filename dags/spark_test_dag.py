from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def task_1():
    print("Task 1 completed")

def task_2():
    print("Task 2 completed")

def task_3():
    print("Task 3 completed")

def task_4():
    print("Task 4 completed")

def pyspark_parquet_to_csv():
    spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()
    input_path = "/mnt/c/data/pyspark_transform.parquet"
    output_path = "/mnt/d/airflow/output_csv"
    df = spark.read.parquet(input_path)
    print("Initial Data:")
    df.show(5)
    df_clean = df.dropna()
    print("Rows after cleaning:", df_clean.count())
    df_clean.describe().show()
    df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"✅ CSV written to {output_path}")
    spark.stop()

default_args = {'owner': 'airflow', 'start_date': datetime(2025, 11, 1)}

dag = DAG(
    'five_task_dag_with_pyspark',
    default_args=default_args,
    description='5 tasks with PySpark job',
    schedule_interval='@daily',
    catchup=False,
)

t1 = PythonOperator(task_id='task_1', python_callable=task_1, dag=dag)
t2 = PythonOperator(task_id='task_2', python_callable=task_2, dag=dag)
t3 = PythonOperator(task_id='task_3', python_callable=task_3, dag=dag)
t4 = PythonOperator(task_id='task_4', python_callable=task_4, dag=dag)
t5 = PythonOperator(task_id='pyspark_parquet_to_csv', python_callable=pyspark_parquet_to_csv, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
