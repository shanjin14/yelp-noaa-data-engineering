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
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

#days_ago(-3) #datetime(2021,7,26,0,0),datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
dag = DAG('stg_to_fdn_fact',
          default_args=default_args,
          description='create fact from staging to foundation',
          dagrun_timeout=timedelta(hours=5),
          start_date=datetime(2019,1,1,0,0,0,0),
          end_date =datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1), 
          schedule_interval='0 3 * * *',
          max_active_runs =1
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



insert_fdn_fact = LoadDataOperator(
    task_id='insert_into_fdn_dim_users',
    dag=dag,
    redshift_conn_id="redshift",
    query="""
    delete from fdn_fact_reviews where date::date between '{{prev_ds}}' and '{{ds}}';
    {}
    where to_timestamp(A.date, 'YYYY-MM-DD HH:MI:SS')::date between '{{prev_ds}}' and '{{ds}}';
    """.format(SqlQueries.fact_insert),
    append_data = True,
    table_name= "fdn_fact_reviews"
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tablelist=["public.fdn_fact_reviews"],
    dq_checks=[
        {'sql': """
        Select coalesce(count(*),0)
        from (
            select date::date,business_id, sum(checkin_count) fdn_cnt from fdn_fact_reviews 
            where date::date between '2021-01-01' and '2021-01-02'
            group by date::date,business_id) A
        join (
            select date::date, business_id, count(*) stg_cnt
            from stg_yelp_checkin
            where date::date between '2021-01-01' and '2021-01-02'
            group by date::date,business_id) B on A.business_id=B.business_id and A.date::date=B.date::date 
        where fdn_cnt!=stg_cnt;""", 'expected_result': 0,"description":"checkin_stats_matched_staging_data"}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> insert_fdn_fact >> run_quality_checks
run_quality_checks >> end_operator

