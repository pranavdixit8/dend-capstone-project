from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries



star_schema_tables = ["immigration_events", "ports"]

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2016, 4, 1),
    'end_date' : datetime(2016, 4, 30)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          # catchup = False,
          description='Load data in Redshift with Airflow',
          schedule_interval='@monthly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



process_files = BashOperator(
    task_id="process_files",
    bash_command = 'python /home/pranav/Desktop/data/projects/dend-capstone-project/etl.py',
    dag = dag)



load_immigration_events_to_redshift = StageToRedshiftOperator(
    task_id='load_immigration_events',
    dag=dag,
    table = "immigration_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "dend-2020-capstone",#change bucket
    # s3_key = "log_data",
    s3_key = "immigration_events/year={execution_date.year}/month={execution_date.month}",
    create_table_sql = SqlQueries.create_table_immigration_events,
    format_mode = "json 'auto' "
)

laod_ports_to_redshift = StageToRedshiftOperator(
    task_id='load_ports',
    dag=dag,
    table = "ports",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "dend-2020-capstone", #change bucket
    s3_key = "ports",
    create_table_sql = SqlQueries.create_table_ports,
    format_mode = "json 'auto'"
)



run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables = star_schema_tables,
    redshift_conn_id = "redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> process_files


process_files >> load_immigration_events_to_redshift

process_files >> laod_ports_to_redshift


load_immigration_events_to_redshift >> run_quality_checks

laod_ports_to_redshift >> run_quality_checks


run_quality_checks >> end_operator
