from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook



class S3DeleteObjectsOperator(BaseOperator):
    ui_color = '#356f81'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key_prefix="",
                 *args, **kwargs):

        super(S3DeleteObjectsOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix

    def chunks(self,lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
            
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)

        object_keys = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key_prefix)
        self.log.info("object keys".format(object_keys))
        if object_keys:
            batches = self.chunks(object_keys, 1000)
            for batch in batches:
                s3_hook.delete_objects(bucket=self.s3_bucket, keys=batch) 
        self.log.info("Delete Completed")






