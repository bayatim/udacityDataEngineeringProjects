from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    staging_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        FORMAT AS json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 destination_table="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.destination_table = destination_table
        self.json_format = json_format

    def execute(self, context):
        self.log.info("Creating aws hook")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Aws hook created")
        
        self.log.info("Creating redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift hook created")
        
        self.log.info("Formating s3 key")
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        
        self.log.info("Start clearing content of destination redshift table")
        redshift.run("DELETE FROM {}".format(self.destination_table))
        self.log.info("Destination redshift table is emptied")
        
        self.log.info('Start loading data from s3 to destination redshift table')
        sql_stmt = self.staging_copy_sql.format(
            self.destination_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        
        redshift.run(sql_stmt)
        self.log.info('Destination redshift table is loaded!')