from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        s3_bucket="s3://udacity-dend/log_data",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_region="us-west-2",
        s3_format="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        s3_bucket="s3://udacity-dend/song_data/A/A/A",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_region="us-west-2",
        s3_format="FORMAT AS JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table="songplays",
        redshift_conn_id="redshift",
        source_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table="users",
        redshift_conn_id="redshift",
        source_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table="songs",
        redshift_conn_id="redshift",
        source_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table="artists",
        redshift_conn_id="redshift",
        source_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="time",
        redshift_conn_id="redshift",
        source_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tests=[
            {
                "query": 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
                "expect": [(0,)]
            },
        ]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator \
        >> [stage_events_to_redshift, stage_songs_to_redshift] \
        >> load_songplays_table \
        >> [
            load_song_dimension_table,
            load_user_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table
        ] \
        >> run_quality_checks \
        >> end_operator


final_project_dag = final_project()
