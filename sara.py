from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import BranchPythonOperator
import sys

sys.path.append('/opt/airflow/includes')
from emp_dim_insert_update import join_and_detect_new_or_changed_rows
from queries import *

def check_rows_to_update(**kwargs):
    rows_to_update = kwargs['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if not rows_to_update:
        return "insert_task"
    else:
        return "update_task"



with DAG("rdd_to2", start_date=datetime(2023, 5, 12), schedule='@hourly', catchup=False) as dag:
    task1 = SqlToS3Operator(
        task_id='transfer_1',
        query='select * from hr.emp_details',
        s3_bucket='staging.emp.data',
        s3_key='sara_emp_details.csv',
        sql_conn_id='rds_11',
        aws_conn_id='sara_conn',
        replace=True
    )

    task2 = SqlToS3Operator(
        task_id='transfer_2',
        query='select * from finance.emp_sal',
        s3_bucket='staging.emp.data',
        s3_key='sara_emp_sal.csv',
        sql_conn_id='rds_11',
        aws_conn_id='sara_conn',
        replace=True
    )
    
    task3 = join_and_detect_new_or_changed_rows()
    
    check = BranchPythonOperator(
        task_id='check_update',
        python_callable=check_rows_to_update
    )
        
    update_task = SnowflakeOperator(
        task_id='update_task',
        sql=UPDATE_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")}}'),
        schema='dimensions',
        snowflake_conn_id='snowflack_conn'
    )

    insert_task = SnowflakeOperator(
        task_id="insert_task",
        sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert")}}'),
        schema='dimensions',
        snowflake_conn_id='snowflack_conn'
    )

    insert_task_to2 = SnowflakeOperator(
        task_id="insert_task_to2",
        sql=INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert")}}'),
        schema='dimensions',
        snowflake_conn_id='snowflack_conn'
    )
   
    check >> update_task >> insert_task_to2
    check >> insert_task
    task1 >> task2 >> task3 >> check
