from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import NOAA_temperature_data as NOAA
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from operators.s3_upload_object import S3UploadObjectOperator
from operators.load_data_redshift import LoadDataOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers.sql_queries import SqlQueries
from operators.data_quality import DataQualityOperator


noaa_conn = BaseHook.get_connection("noaa_token")
noaa_key = noaa_conn.password

default_args = {
    'owner': 'george',
    'start_date': datetime(2019, 1, 12,0,0,0,0),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

#days_ago(-3) #datetime(2021,7,26,0,0),datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
dag = DAG('NOAA_raw_stg',
          default_args=default_args,
          description='acquire data from NOAA API and load to redshift',
          dagrun_timeout=timedelta(hours=5),
          start_date=datetime(2020,12,31,0,0,0,0),
          end_date =datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1), 
          schedule_interval='0 3 * * *',
          catchup=True,
          max_active_runs =1
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

acquite_noaa_data_task = PythonOperator(
    task_id='get_data_from_NOAA_API',
    dag=dag,
    python_callable=NOAA.acquire_temperature_data,
    op_kwargs={'inToken':noaa_key},
    provide_context=True
)

stage_to_redshift_minus_2_day = StageToRedshiftOperator(
    task_id='Stage_noaa_weather_to_redshift_t_minus_2',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="ldg_noaa_weathers",
    s3_bucket="dend-data-landing",
    s3_key='ldg_noaa/{{ (execution_date - macros.timedelta(days=4)).year }}/{{ "{:02}".format((execution_date - macros.timedelta(days=4)).month) }}/{{ "{:02}".format((execution_date - macros.timedelta(days=4)).day) }}/temperature_data.csv',
    json_string="IGNOREHEADER 1",
    formattype="csv",
    truncate=True
)

stage_to_redshift_minus_1_day = StageToRedshiftOperator(
    task_id='Stage_noaa_weather_to_redshift_t_minus_1',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="ldg_noaa_weathers",
    s3_bucket="dend-data-landing",
    s3_key='ldg_noaa/{{ (execution_date - macros.timedelta(days=3)).year }}/{{ "{:02}".format((execution_date - macros.timedelta(days=3)).month) }}/{{ "{:02}".format((execution_date - macros.timedelta(days=3)).day) }}/temperature_data.csv',
    json_string="IGNOREHEADER 1",
    formattype="csv",
    truncate=False
)

stage_to_redshift_minus_0_day = StageToRedshiftOperator(
    task_id='Stage_noaa_weather_to_redshift_t_minus_0',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="ldg_noaa_weathers",
    s3_bucket="dend-data-landing",
    s3_key='ldg_noaa/{{ (execution_date - macros.timedelta(days=2)).year }}/{{ "{:02}".format((execution_date - macros.timedelta(days=2)).month) }}/{{ "{:02}".format((execution_date - macros.timedelta(days=2)).day) }}/temperature_data.csv',
    json_string="IGNOREHEADER 1",
    formattype="csv",
    truncate=False
)

load_to_stg = LoadDataOperator(
    task_id='Upsert_from_landing_to_staging_deduplicated',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.stg_noaa_upsert,
    append_data = True,
    table_name= "stg_noaa_weathers"
)


upload_s3_task_minus_2_day = S3UploadObjectOperator(  
        task_id='upload_object_to_s3_d_minus_2',
        s3_bucket='dend-data-landing',
        s3_key_prefix='ldg_noaa/{{ execution_date.year }}/{{ "{:02}".format(execution_date.month) }}/{{ "{:02}".format(execution_date.day - 4) }}',
        aws_credentials_id='aws_default',
        file_path='{{ execution_date.year }}/{{ "{:02}".format(execution_date.month) }}/{{ "{:02}".format(execution_date.day - 4) }}/temperature_data.csv',
        dag=dag)  

upload_s3_task_minus_1_day = S3UploadObjectOperator(  
        task_id='upload_object_to_s3_d_minus_1',
        s3_bucket='dend-data-landing',
        s3_key_prefix='ldg_noaa/{{ execution_date.year }}/{{ "{:02}".format(execution_date.month) }}/{{ "{:02}".format(execution_date.day - 3) }}',
        aws_credentials_id='aws_default',
        file_path='{{ execution_date.year }}/{{ "{:02}".format(execution_date.month) }}/{{ "{:02}".format(execution_date.day - 3) }}/temperature_data.csv',
        dag=dag) 

upload_s3_task_minus_0_day = S3UploadObjectOperator(  
        task_id='upload_object_to_s3_d',
        s3_bucket='dend-data-landing',
        s3_key_prefix='ldg_noaa/{{ execution_date.year }}/{{ "{:02}".format(execution_date.month) }}/{{ "{:02}".format(execution_date.day-2) }}',
        aws_credentials_id='aws_default',
        file_path='{{ execution_date.year }}/{{ "{:02}".format(execution_date.month) }}/{{ "{:02}".format(execution_date.day-2) }}/temperature_data.csv',
        dag=dag) 


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tablelist=["public.stg_noaa_weathers"],
    dq_checks=[
        {'sql': "Select coalesce(count(distinct date),0) from (select date,location_id,station,count(*) cnt from stg_noaa_weathers group by date,location_id,station having count(*)>1)", 'expected_result': 0,"description":"non_unique_row"},
        {'sql': "select datediff(day,min(date)::timestamp,max(date)::timestamp)+1 - count(distinct date) from stg_noaa_weathers;", 'expected_result': 0,"description":"missing_date"}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> acquite_noaa_data_task 
acquite_noaa_data_task >> upload_s3_task_minus_2_day 
acquite_noaa_data_task >> upload_s3_task_minus_1_day 
acquite_noaa_data_task >> upload_s3_task_minus_0_day 
upload_s3_task_minus_2_day >> stage_to_redshift_minus_2_day
upload_s3_task_minus_1_day >> stage_to_redshift_minus_2_day
upload_s3_task_minus_0_day >> stage_to_redshift_minus_2_day
stage_to_redshift_minus_2_day >> stage_to_redshift_minus_1_day
stage_to_redshift_minus_2_day >> stage_to_redshift_minus_0_day
stage_to_redshift_minus_1_day >> load_to_stg
stage_to_redshift_minus_0_day >> load_to_stg
load_to_stg >> run_quality_checks
run_quality_checks >> end_operator

