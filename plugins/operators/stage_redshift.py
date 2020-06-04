import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#
# Operator populates staging_events_table by copying 
# JSON files from  S3 to Amazon Redshift
#
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # templated sql to copy json from S3 to redshift
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """
  

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",                 
                 aws_credentials_id="",
                 table="",
                 append=False,
                 s3_bucket="",
                 s3_prefix="",
                 s3_json="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.append = append
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_json = s3_json

    def execute(self, context):
        # acquire hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # clear table if not in append mode 
        if not self.append:
            logging.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        # create S3 path and format sql
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_prefix)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_json
        )

        # execute the S3 to Redshift Copy
        logging.info(formatted_sql)
        redshift.run(formatted_sql)





