from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from operators.load_data_redshift import LoadDataOperator
from helpers.sql_queries import SqlQueries
from operators.data_quality import DataQualityOperator



default_args = {
    'owner': 'george',
    'start_date': datetime(2019, 1, 12,0,0,0,0),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

#days_ago(-3) #datetime(2021,7,26,0,0),datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
dag = DAG('stg_to_fdn_dimensions',
          default_args=default_args,
          description='create dimensions from staging to foundation',
          dagrun_timeout=timedelta(hours=5),
          start_date=days_ago(0),
          schedule_interval='0 3 * * *',
          max_active_runs =1
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


#append_data = False, do truncate and load
insert_fdn_dim_business = LoadDataOperator(
    task_id='insert_into_fdn_dim_business',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.dim_business_insert,
    append_data = False,
    table_name= "fdn_dim_business"
)


insert_fdn_dim_weather = LoadDataOperator(
    task_id='insert_into_fdn_dim_weather',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.dim_weather_insert,
    append_data = False,
    table_name= "fdn_dim_weather"
)

insert_fdn_dim_users = LoadDataOperator(
    task_id='insert_into_fdn_dim_users',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.dim_users_insert,
    append_data = False,
    table_name= "fdn_dim_users"
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tablelist=["public.fdn_dim_users","public.fdn_dim_business","public.fdn_dim_weather"],
    dq_checks=[
         {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM fdn_dim_users", 'expected_result': 1,"description":"large_than_zero"},
         {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM fdn_dim_business", 'expected_result': 1,"description":"large_than_zero"},
         {'sql': "SELECT COUNT(*)/(COUNT(*)+0.001) FROM fdn_dim_weather", 'expected_result': 1,"description":"large_than_zero"},     
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> insert_fdn_dim_business >> run_quality_checks
start_operator >> insert_fdn_dim_weather >> run_quality_checks
start_operator >> insert_fdn_dim_users >> run_quality_checks
run_quality_checks >> end_operator

