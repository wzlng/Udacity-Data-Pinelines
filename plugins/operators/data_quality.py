from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tests=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tests = tests
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting Data Quality check...')
        for test in self.tests:
            query = test['query']
            expected = test['expect']

            query_result = redshift.get_records(query)

            if query_result == expected:
                self.log.info(f'{query} - Passed!')
            else:
                raise ValueError(
                    f'{query} - Failed!\nExpected: {expected}\nActual: {query_result}')

        self.log.info('Data Quality check finished!')
