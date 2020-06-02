from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries
from helpers import sql_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.utcnow()
}

dag = DAG('dend_dpl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='@monthly'
        )

#
# Operators
#

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_stage_events_table = PostgresOperator(
    task_id="create_stage_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.staging_events_table_create
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events_table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_prefix="log-data",
    s3_json='s3://udacity-dend/log_json_path.json'
)

create_stage_songs_table = PostgresOperator(
    task_id="create_stage_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.staging_songs_table_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs_table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_prefix="song-data"    
)

create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.songplay_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.user_table_create
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="times",
    sql=SqlQueries.time_table_insert
)

create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.song_table_create
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert
)

create_artists_table = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.artist_table_create
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert
)

create_times_table = PostgresOperator(
    task_id="create_times_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_tables.time_table_create
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id="redshift",
    tables=[
        'staging_events_table', 
        'users', 
        'times', 
        'staging_songs_table', 
        'songs', 
        'artists',
        'songplays'
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task Dependencies
#
start_operator >> create_stage_events_table
start_operator >> create_stage_songs_table

create_stage_events_table >> stage_events_to_redshift
create_stage_songs_table >> stage_songs_to_redshift

stage_events_to_redshift >> create_songplays_table
stage_songs_to_redshift >> create_songplays_table
create_songplays_table >> load_songplays_table

load_songplays_table >> create_users_table
load_songplays_table >> create_songs_table
load_songplays_table >> create_artists_table
load_songplays_table >> create_times_table

# stage_events_to_redshift >> create_users_table
# stage_events_to_redshift >> create_times_table
create_users_table >> load_user_dimension_table
create_times_table >> load_time_dimension_table

# stage_songs_to_redshift >> create_songs_table
# stage_songs_to_redshift >> create_artists_table
create_songs_table >> load_song_dimension_table
create_artists_table >> load_artist_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator