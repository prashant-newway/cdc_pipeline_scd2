import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook        # intermediate


source_tables = ['customer' , 'customer_address' , 'customer_payment']


with DAG('etl_db_to_db',
         start_date = datetime(2024,10,3)
         , schedule_interval = '@daily',
         catchup= False,
         template_searchpath =  os.path.join(os.getcwd(),'include','sql','etl_db_to_db')
         ) as dag:
    
    @task(multiple_outputs=True)
    def get_dag_conf(**context):
        dag_conf = Variable.get('customer', deserialize_json=True, default_var={})
        load_type = dag_conf.get('load_type', None)
        print(dag_conf)
        print(context['dag_run'].run_type)
        if context['dag_run'].run_type == "scheduled" and load_type == 'full':
            raise ValueError("Full run can't be scheduled!!! Might be left over from a previous run. Aborting...")
        
        if load_type == 'full':
            where_cond = None
        elif load_type == 'delta':
            where_cond = " where updated_at > '{max_date}'"
        else:
            raise ValueError("Invalid load type")

        return {"where_cond": where_cond, "load_type": load_type}

    dag_conf = get_dag_conf()

    @task.branch
    def check_full_load(load_type:str):
        if load_type == 'full':
            return ['full_transform','full_load']
        elif load_type == 'delta':
            return ['transform','cdc_load']
    
    check_full_load = check_full_load(dag_conf['load_type'])

    full_transform = SQLExecuteQueryOperator(
        task_id = 'full_transform',
        conn_id = 'snowflake',
        sql = 'full_customer_transform.sql',
    )

    full_load = SQLExecuteQueryOperator(
        task_id='full_load',
        conn_id='snowflake',
        sql='full_load.sql'
    )