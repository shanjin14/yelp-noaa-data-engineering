from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers.sql_queries import SqlQueries
from operators.stage_redshift import StageToRedshiftOperator
from operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'george',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('yelp_raw_stg',
          default_args=default_args,
          description='Move yelp data from raw to staging',
          dagrun_timeout=timedelta(hours=2),
          start_date=days_ago(0),
          schedule_interval='0 3 * * *',
          catchup=False)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_business_to_redshift = StageToRedshiftOperator(
    task_id='Stage_business',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="stg_yelp_business",
    s3_bucket="dend-data-raw",
    s3_key="yelp-data/yelp_academic_dataset_business.json",
    json_string="'s3://dend-data-raw/yelp-data/businessjsonpath.json'",
    formattype="json"
)

stage_reviews_to_redshift = StageToRedshiftOperator(
    task_id='Stage_reviews',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="stg_yelp_reviews",
    s3_bucket="dend-data-raw",
    s3_key="yelp-data/yelp_academic_dataset_review.json",
    json_string="'auto'",
    formattype="json"
)

#PARQUET FILE DO NOT NEED JSON STRING, and don't have statupdate
stage_checkin_to_redshift = StageToRedshiftOperator(
    task_id='Stage_checkin',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="stg_yelp_checkin",
    s3_bucket="dend-data-landing",
    s3_key="stg_checkin/",
    json_string="",
    formattype="PARQUET",
    statupdate=""
)

#PARQUET FILE DO NOT NEED JSON STRING,     statupdate="" PARQUET does not support statupdate
stage_users_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="stg_yelp_users",
    s3_bucket="dend-data-landing",
    s3_key="stg_users/",
    json_string="",
    formattype="PARQUET",
    statupdate=""
)

stage_users_friends_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users_friends',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    table="stg_yelp_users_friends",
    s3_bucket="dend-data-landing",
    s3_key="stg_users_friends/",
    json_string="",
    formattype="PARQUET",
    statupdate=""
)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

SPARK_STEPS = [
    {
        'Name': "{{params.script_arg_input}}",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
                        "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                "--func",
                "{{params.script_arg_input}}"
            ]
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'PySparkJobtry3',
    'ReleaseLabel': 'emr-5.29.0',
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", 
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        dag=dag,
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )
    
delete_s3_files_checkin = S3DeleteObjectsOperator (  
        task_id='delete_s3_stg_checkin_landing',
        s3_bucket='dend-data-landing',
        s3_key_prefix='stg_checkin',
        aws_credentials_id='aws_default',
        dag=dag)  

delete_s3_files_users = S3DeleteObjectsOperator (  
        task_id='delete_s3_stg_users_landing',
        s3_bucket='dend-data-landing',
        s3_key_prefix='stg_users',
        aws_credentials_id='aws_default',
        dag=dag)  

step_adder = EmrAddStepsOperator(
        task_id='add_steps_run_pyspark_etl_checkin',
        dag=dag,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
        params={ 
        "BUCKET_NAME": "dend-code",
        "s3_script": "raw_stg_etl.py",
        "script_arg_input": "process_yelp_checkin"
    }
    )

step_adder2 = EmrAddStepsOperator(
        task_id='add_steps_run_pyspark_etl_users',
        dag=dag,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
        params={ 
        "BUCKET_NAME": "dend-code",
        "s3_script": "raw_stg_etl.py",
        "script_arg_input": "process_yelp_users"
    }
    )

step_checker = EmrStepSensor(
        task_id='watch_step_etl_checkin',
        dag=dag,
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_run_pyspark_etl_checkin', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

step_checker2 = EmrStepSensor(
        task_id='watch_step2_etl_users',
        dag=dag,
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_run_pyspark_etl_users', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_emr_cluster',
        dag=dag,
        trigger_rule='all_done',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> cluster_creator 
cluster_creator >> delete_s3_files_checkin 
cluster_creator >> delete_s3_files_users
delete_s3_files_users >> step_adder 
delete_s3_files_checkin >> step_adder
step_adder >> step_adder2 
step_adder2 >> step_checker 
step_adder2 >> step_checker2
step_checker >> cluster_remover 
step_checker2 >> cluster_remover 
cluster_remover >> stage_checkin_to_redshift
cluster_remover >> stage_users_friends_to_redshift
cluster_remover >> stage_users_to_redshift
stage_users_to_redshift >> end_operator
stage_checkin_to_redshift >> end_operator
stage_users_friends_to_redshift >> end_operator
start_operator >> stage_business_to_redshift  >> end_operator
start_operator >> stage_reviews_to_redshift  >> end_operator

