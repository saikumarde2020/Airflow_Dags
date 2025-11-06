from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# ------------------------------
# DAG Default Arguments
# ------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# ------------------------------
# File Paths (mapped for Ubuntu WSL)
# ------------------------------
# Windows C:\data\usdata.csv  ->  /mnt/c/data/usdata.csv
# Windows D:\airflow\filtered_usdata.xml  ->  /mnt/d/airflow/filtered_usdata.xml

INPUT_PATH = "/mnt/c/data/usdata.csv"
OUTPUT_PATH = "/mnt/d/airflow/filtered_usdata.xml"

# ------------------------------
# Python Transformation Function
# ------------------------------
def filter_and_export_to_xml():
    print(f"📂 Reading CSV from: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)

    # Clean up county names
    df['county'] = df['county'].astype(str).str.strip()

    # Filter for Dallas county
    df_filtered = df[df['county'].str.lower() == 'dallas']

    # Remove duplicates
    df_filtered = df_filtered.drop_duplicates()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Write filtered data to XML
    df_filtered.to_xml(OUTPUT_PATH, index=False, root_name='records', row_name='record')

    print(f"✅ XML successfully created at: {OUTPUT_PATH}")
    print(f"✅ Rows written: {len(df_filtered)}")

# ------------------------------
# DAG Definition
# ------------------------------
with DAG(
    dag_id='filter_usdata_to_xml_dag',
    default_args=default_args,
    description='Filter Dallas county data and export to XML format',
    schedule_interval=None,
    start_date=datetime(2025, 11, 5),
    catchup=False,
    tags=['csv', 'xml', 'pandas', 'local'],
) as dag:

    transform_task = PythonOperator(
        task_id='filter_and_convert_to_xml',
        python_callable=filter_and_export_to_xml
    )

    transform_task
