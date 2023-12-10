from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 sql="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(RedshiftOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting Redshift operator...')
        sql_statements = self.sql.split(';')
        for statement in sql_statements:
            if statement.strip():
                redshift.run(statement)

        self.log.info('SQL file executed successfully.')
