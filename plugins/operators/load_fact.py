from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    load_query = ("""
        INSERT INTO {table}
        {values}
    """)

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 source_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.source_query = source_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading fact table ('{self.table}') into Redshift...")
        formated_query = LoadFactOperator.load_query.format(
            table=self.table,
            values=self.source_query
        )
        redshift.run(formated_query)
        self.log.info(f"Success! Redshift table: '{self.table}'")
