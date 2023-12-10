from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    load_query = ("""
        INSERT INTO {table}
        {values}
    """)

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 truncate=False,
                 source_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate = truncate
        self.source_query = source_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(
                f"Clearing data from destination Redshift table: '{self.table}'")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(
            f"Loading dimension table ('{self.table}') into Redshift...")
        formated_query = LoadDimensionOperator.load_query.format(
            table=self.table,
            values=self.source_query
        )
        redshift.run(formated_query)
        self.log.info(f"Success! Redshift table: '{self.table}'")
