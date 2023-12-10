from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_query = ("""
        COPY {table}
        FROM '{s3_bucket}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'         
        REGION '{s3_region}'
        {s3_format}
        COMPUPDATE OFF
        TIMEFORMAT AS 'epochmillisecs';
    """)

    @apply_defaults
    def __init__(self,
                 table="",
                 s3_bucket="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_region="",
                 s3_format="",
                 * args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_region = s3_region
        self.s3_format = s3_format

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(
            self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(
            f"Clearing data from destination Redshift table: '{self.table}'")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift...")
        formated_sql = StageToRedshiftOperator.copy_query.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            access_key_id=aws_connection.login,
            secret_access_key=aws_connection.password,
            s3_region=self.s3_region,
            s3_format=self.s3_format
        )
        redshift.run(formated_sql)
        self.log.info(f"Success! Redshift table: '{self.table}'")
