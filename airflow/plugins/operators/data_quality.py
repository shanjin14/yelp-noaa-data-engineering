from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tablelist ="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tablelist = tablelist
        self.dq_checks=dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for chk in  self.dq_checks:
            qry=chk['sql']
            result=chk['expected_result']
            checkdesc=chk['description']
            records = redshift_hook.get_records(qry)
            self.log.info(f"expected result {result} compare with check result {records}")
            if abs(result-records[0][0])>0.001:
                raise ValueError(f"Data quality check failed. {qry} {checkdesc} failed")
            self.log.info(f"Data quality on table {qry} {checkdesc} check passed")
            