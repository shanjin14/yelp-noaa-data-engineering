from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import os


class S3UploadObjectOperator(BaseOperator):
    ui_color = '#356f81'
    template_fields = ['file_path','s3_key_prefix']
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key_prefix="",
                 file_path ="",
                 *args, **kwargs):

        super(S3UploadObjectOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.file_path=file_path


            
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)

        self.log.info(f'Upload to:  {self.s3_bucket}')
        file_name = os.path.split(self.file_path)[1]
        self.log.info(self.file_path)
        s3_key = '{}/{}'.format(self.s3_key_prefix,file_name)
        self.log.info(s3_key)
        if os.path.exists(self.file_path):
            s3_hook.load_file(
                filename=self.file_path,
                key=s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            ) 
            self.log.info("Upload Completed")
        else:
            self.log.info("file not exist")
            






