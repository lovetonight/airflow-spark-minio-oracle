import json
import os
import requests
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.macros import ds_format
from datetime import date, timedelta, datetime
from airflow.models import Variable





# S3
S3_BUCKET = "codingtest"
S3_RAW_KEY = "raw"
S3_CONN = 'minio_conn'

# ORACLE
jar_path = "/usr/local/spark/assets/jars/"
jars = ["ojdbc8-19.3.0.0.jar", "aws-java-sdk-bundle-1.11.375.jar", "hadoop-aws-3.2.0.jar"]
ORACLE_DRIVER_JAR = ",".join(f"{jar_path}{jar}" for jar in jars)
ORACLE_USER = "BAI_TEST"
ORACLE_PASSWORD = Variable.get("ORACLE_PASSWORD")
ORACLE_URL = "jdbc:oracle:thin:@oracle-xe:1521:XE"
ORACLE_TABLE = 'Weather'


FILE_NAME = 'data.json'
STORAGE = "/tmp/data"
API_URL = 'https://api.openweathermap.org/data/2.5/forecast?appid={}&lat={}&lon={}'
LAT = '21.0278'
LON = '105.8342'
APPID = Variable.get("WEATHER_API_KEY")
spark_master = "spark://spark:7077"


# Support function


def get_date_part(current_date: date) -> str:
    return ds_format(current_date, '%Y-%m-%d', '%Y/%m/%d/')


def generate_filename_path(current_date: str, filename: str) -> str:
    date_part = get_date_part(current_date)
    storage_dir = os.path.join(STORAGE, date_part)
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    file_path = os.path.join(storage_dir, filename)
    return file_path


def generate_dest_path(key: str, current_date: str, filename: str) -> str:
    date_part = get_date_part(current_date)
    dest_path = os.path.join(key, date_part, filename)
    return dest_path


# Python Callable
def get_data(**kwargs) -> None:
    filename = kwargs.get("filename")
    url = kwargs.get("url")
    current_date = kwargs.get('ds')

    # Get Data
    response = requests.get(url)
    data = response.json()
    data = data['list']

    # Generate Path
    file_path = generate_filename_path(current_date, filename)

    with open(file_path, 'w') as f:
        json.dump(list(data), f, indent=4)


def upload_file(**kwargs) -> None:
    # Get param
    s3_conn = kwargs.get("s3_conn")
    bucket = kwargs.get("bucket")
    filename = kwargs.get("filename")
    current_date = kwargs.get('ds')

    # Generate Path
    file_path = generate_filename_path(current_date, filename)
    dest_path = generate_dest_path(S3_RAW_KEY, current_date, filename)
    print(file_path, dest_path)
    # dest_path = os.path.join(S3_RAW_KEY, date_part, filename)
    # Connect

    s3 = S3Hook(s3_conn)
    s3.load_file(
        file_path,
        key=dest_path,
        bucket_name=bucket,
        replace=True)


# Path
dest_path = generate_dest_path(
    S3_RAW_KEY, datetime.today().strftime('%Y-%m-%d'), FILE_NAME)

# Dag
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 15),
    'retries': 1
}

dag = DAG(
    'minio_spark',
    default_args=default_args,
    schedule='@daily'
)

get_data_from_url = PythonOperator(
    task_id='get_data',
    provide_context=True,
    python_callable=get_data,
    op_kwargs={
        'url': API_URL.format(APPID, LAT, LON),
        'filename': FILE_NAME
    },
    dag=dag
)
upload_to_minio = PythonOperator(
    task_id='upload_to_minio',
    provide_context=True,
    python_callable=upload_file,
    op_kwargs={
        'bucket': S3_BUCKET,
        's3_conn': S3_CONN,
        'filename': FILE_NAME
    },
    dag=dag
)

read_from_oracle = SparkSubmitOperator(
    task_id='spark_job_load_oracle',
    application="/usr/local/spark/applications/load_oracle.py",
    name="load-oracle",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[ORACLE_URL, ORACLE_TABLE,
                      ORACLE_USER, ORACLE_PASSWORD, S3_BUCKET, dest_path],
    jars=ORACLE_DRIVER_JAR,
    driver_class_path=ORACLE_DRIVER_JAR,
    dag=dag)

get_data_from_url >> upload_to_minio >> read_from_oracle