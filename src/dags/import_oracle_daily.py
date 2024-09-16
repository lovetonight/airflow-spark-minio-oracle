from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

from datetime import datetime
import pandas as pd
import numpy as np

KPI_file = "/usr/local/spark/assets/data/KPI Template.xlsx"
modeling_file = "/usr/local/spark/assets/data/Modeling and DAX.xlsx"
tables_queries = {
    'KPI': '''
        CREATE TABLE KPI (
            YEAR NUMBER(4) NOT NULL,
            BRANCH VARCHAR2(25) NOT NULL,
            KPI_VALUE NUMBER(15) NOT NULL
        )
    ''',
    'CUSTOMER': '''
        CREATE TABLE CUSTOMER(
            CUSTOMERID VARCHAR2(30) PRIMARY KEY,
            CUSTOMERNAME VARCHAR2(255) NOT NULL
        )
    ''',
    'BRANCH': '''
        CREATE TABLE BRANCH(
            BranchID VARCHAR2(30) PRIMARY KEY,
            BranchName VARCHAR2(255) NOT NULL,
            City VARCHAR2(30) NOT NULL
        )
    ''',
    'STAFF': '''
        CREATE TABLE STAFF(
            StaffID VARCHAR2(30) PRIMARY KEY,
            StaffName VARCHAR2(255) NOT NULL
        )
    ''',
    'PRODUCT': '''
        CREATE TABLE PRODUCT(
            ProductID VARCHAR2(30) PRIMARY KEY,
            ProductName VARCHAR2(300) NOT NULL,
            ProductGroup VARCHAR2(30) NOT NULL
        )
    ''',
    'SALE': '''
        CREATE TABLE SALE(
            AccountingDate DATE,
            OrderID VARCHAR2(30) NOT NULL,
            CustomerID VARCHAR2(30) NOT NULL,
            ProductID VARCHAR2(30) NOT NULL,
            Quantity NUMBER(19) NOT NULL,
            UnitPrice NUMBER(15, 2) NOT NULL,
            Revenue NUMBER(19) NOT NULL,
            Cost NUMBER(15, 2) NOT NULL,
            StaffID VARCHAR2(30) NOT NULL,
            BranchID VARCHAR2(30) NOT NULL
        )
    '''
}
CHECK_EXIST_QUERY = '''
    SELECT 1 FROM all_tables WHERE table_name = '{}'
'''

sheet_names = {
    'Khách hàng': 'customer_df',
    'Sản phẩm': 'product_df',
    'Nhân viên': 'staff_df',
    'Dữ liệu bán hàng': 'sale_df',
    'Chi nhánh': 'branch_df'
}


def query_oracle():
    conn_id = 'oracle_conn'
    oracle_hook = OracleHook(oracle_conn_id=conn_id)
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()

    # Create table
    for table_name, create_query in tables_queries.items():
        cursor.execute(CHECK_EXIST_QUERY.format(table_name.upper()))
        if not cursor.fetchone():
            cursor.execute(create_query)
            print(f'{table_name} table created')

    dataframes = {name: pd.read_excel(
        modeling_file, sheet_name=sheet) for sheet, name in sheet_names.items()}
    KPI_df = pd.read_excel(KPI_file, sheet_name='KPI theo năm')
    KPI_df['KPI'] = KPI_df['KPI'].str.replace(
        ",", "", regex=True).astype(np.int64)

    customer_df = dataframes['customer_df']
    product_df = dataframes['product_df']
    staff_df = dataframes['staff_df']
    sale_df = dataframes['sale_df']
    branch_df = dataframes['branch_df']
    df_list = {
        'CUSTOMER': customer_df,
        'PRODUCT': product_df,
        'STAFF': staff_df,
        'SALE': sale_df,
        'BRANCH': branch_df,
        'KPI': KPI_df
    }

    # Insert Data

    for table_name, df in df_list.items():
        # Get new record
        select_sql = f"SELECT * FROM {table_name}"
        df_sql = pd.read_sql(select_sql, con=conn)
        df.columns = df_sql.columns
        df_combined = pd.merge(df, df_sql, how='left', indicator=True)
        df_diff = df_combined[df_combined['_merge']
                              == 'left_only'].drop(columns=['_merge'])
        # Insert
        if df_diff.shape[0] > 0:
            rows = [tuple(x) for x in df_diff.to_numpy()]
            insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join([':' + str(i) for i in range(1, len(df_diff.columns) + 1)])})"
            cursor.executemany(insert_sql, rows)
            print(f"inserted table {table_name}")
    # TODO: Add duplicate primary key
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
