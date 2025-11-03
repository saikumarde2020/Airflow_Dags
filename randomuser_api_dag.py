from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
from pyspark.sql import SparkSession

# ----------------------------
# DAG Configuration
# ----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

DATA_PATH = "/home/dellsai/airflow/api_output"
os.makedirs(DATA_PATH, exist_ok=True)

# ----------------------------
# Python function 1: Fetch Random User API
# ----------------------------
def fetch_random_users():
    url = "https://randomuser.me/api/?results=10"  # 10 users
    response = requests.get(url)
    data = response.json()["results"]

    json_path = os.path.join(DATA_PATH, "random_users_raw.json")
    with open(json_path, "w") as f:
        for record in data:
            json.dump(record, f)
            f.write("\n")
    print(f"✅ Saved {len(data)} records to {json_path}")

# ----------------------------
# Python function 2: Clean Data
# ----------------------------
def clean_json():
    raw_path = os.path.join(DATA_PATH, "random_users_raw.json")
    clean_path = os.path.join(DATA_PATH, "random_users_clean.json")

    with open(raw_path, "r") as infile, open(clean_path, "w") as outfile:
        for line in infile:
            record = json.loads(line)
            cleaned = {
                "first_name": record["name"]["first"],
                "last_name": record["name"]["last"],
                "gender": record["gender"],
                "country": record["location"]["country"],
                "email": record["email"],
            }
            json.dump(cleaned, outfile)
            outfile.write("\n")
    print(f"✅ Cleaned data written to {clean_path}")

# ----------------------------
# Python function 3: PySpark JSON → CSV
# ----------------------------
def pyspark_json_to_csv():
    spark = SparkSession.builder \
        .appName("JSON_to_CSV") \
        .master("local[*]") \
        .getOrCreate()

    input_path = os.path.join(DATA_PATH, "random_users_clean.json")
    output_path = os.path.join(DATA_PATH, "random_users_csv")

    df = spark.read.json(input_path)
    df.show(5)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    print(f"✅ CSV written to {output_path}")
    spark.stop()

# ----------------------------
# DAG Definition
# ----------------------------
with DAG(
    dag_id="randomuser_pyspark_dag",
    default_args=default_args,
    description="Fetch Random User API data, clean it, and convert to CSV via PySpark",
    schedule_interval=None,  # manual trigger
    start_date=datetime(2025, 11, 2),
    catchup=False,
    tags=["api", "pyspark", "json_to_csv"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_random_user_api",
        python_callable=fetch_random_users,
    )

    task_clean = PythonOperator(
        task_id="clean_json_data",
        python_callable=clean_json,
    )

    task_pyspark = PythonOperator(
        task_id="pyspark_json_to_csv",
        python_callable=pyspark_json_to_csv,
    )

    # DAG Dependencies
    task_fetch >> task_clean >> task_pyspark
