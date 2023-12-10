from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import RedshiftOperator

__location__ = os.path.realpath(os.path.join(
    os.getcwd(), os.path.dirname(__file__)))

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}


@dag(
    default_args=default_args,
    description='Create tables in Redshift with Airflow',
    schedule_interval='@once'
)
def create_tables():

    start_operator = DummyOperator(task_id='Begin_execution')

    sql = open(os.path.join(__location__, 'create_tables.sql'), 'r').read()

    create_redshift_tables = RedshiftOperator(
        task_id='Create_tables',
        redshift_conn_id='redshift',
        sql=sql
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> create_redshift_tables >> end_operator


create_tables_dag = create_tables()
