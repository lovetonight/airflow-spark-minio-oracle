from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

from datetime import datetime
import pandas as pd
import numpy as np

KPI_file = "/usr/local/spark/assets/data/KPI Template.xlsx"
modeling_file = "/usr/local/spark/assets/data/Modeling and DAX.xlsx"
oracle_table = 'KPI'

def query_oracle():
    conn_id = 'oracle_conn'
    oracle_hook = OracleHook(oracle_conn_id=conn_id)
    conn = oracle_hook.get_conn()

    cursor = conn.cursor()

    KPI_pdf = pd.read_excel(KPI_file, sheet_name='KPI theo năm')
    rows = [tuple(x) for x in KPI_pdf.to_numpy()]
    insert_sql = f"INSERT INTO {oracle_table} VALUES ({', '.join([':' + str(i) for i in range(1, len(KPI_pdf.columns) + 1)])})"
    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'import_data',
    default_args=default_args,
    description='Run at 10:00 AM every day',
    schedule_interval='0 10 * * *',
    catchup=False,
)

run_query = PythonOperator(
    task_id='import_to_oracle',
    python_callable=query_oracle,
    dag=dag
)

