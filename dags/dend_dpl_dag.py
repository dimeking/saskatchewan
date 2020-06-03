from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

from stage_redshift_subdag import get_stage_redshift_dag
from load_dim_subdag import get_load_dim_dag
from load_fact_subdag import get_load_fact_dag
from data_quality_subdag import get_data_quality_dag

from helpers import SqlQueries
from helpers import sql_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'dend-hr',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup_by_default' : False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)    
}

dag_name = 'dend_dpl_dag' 
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

#
# Operators & SubDAGs
#

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_task_id = "Stage_events"
stage_events_subdag_task = SubDagOperator(
    subdag=get_stage_redshift_dag(
        parent_dag_name=dag_name,
        task_id=stage_events_task_id,
        conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events_table",
        append=False,
        create_sql=sql_tables.staging_events_table_create,
        s3_bucket="udacity-dend",
        s3_prefix="log_data",
        s3_json='s3://udacity-dend/log_json_path.json',
        default_args=default_args
    ),
    task_id=stage_events_task_id,
    dag=dag
)

stage_songs_task_id = "Stage_songs"
stage_songs_subdag_task = SubDagOperator(
    subdag=get_stage_redshift_dag(
        parent_dag_name=dag_name,
        task_id=stage_songs_task_id,
        conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs_table",
        append=False,
        create_sql=sql_tables.staging_songs_table_create,
        s3_bucket="udacity-dend",
        s3_prefix="song_data",
        s3_json='auto',
        default_args=default_args
    ),
    task_id=stage_songs_task_id,
    dag=dag
)

# create_stage_songs_table = PostgresOperator(
#     task_id="create_stage_songs_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=sql_tables.staging_songs_table_create
# )

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag,
#     table="staging_songs_table",
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     s3_bucket="udacity-dend",
#     s3_prefix="song_data"    
# )

load_songplays_task_id = "Load_songplays_fact_table"
load_songplays_subdag_task = SubDagOperator(
    subdag=get_load_fact_dag(
        parent_dag_name=dag_name,
        task_id=load_songplays_task_id,
        conn_id="redshift",
        table="songplays",
        append=False,
        create_sql=sql_tables.songplay_table_create,
        load_sql=SqlQueries.songplay_table_insert,
        default_args=default_args
    ),
    task_id=load_songplays_task_id,
    dag=dag
)

load_users_task_id = "Load_user_dim_table"
load_users_subdag_task = SubDagOperator(
    subdag=get_load_dim_dag(
        parent_dag_name=dag_name,
        task_id=load_users_task_id,
        conn_id="redshift",
        table="users",
        append=False,
        create_sql=sql_tables.user_table_create,
        load_sql=SqlQueries.user_table_insert,
        default_args=default_args
    ),
    task_id=load_users_task_id,
    dag=dag
)

load_times_task_id = "Load_time_dim_table"
load_times_subdag_task = SubDagOperator(
    subdag=get_load_dim_dag(
        parent_dag_name=dag_name,
        task_id=load_times_task_id,
        conn_id="redshift",
        table="times",
        append=False,
        create_sql=sql_tables.time_table_create,
        load_sql=SqlQueries.time_table_insert,
        default_args=default_args
    ),
    task_id=load_times_task_id,
    dag=dag
)

load_songs_task_id = "Load_song_dim_table"
load_songs_subdag_task = SubDagOperator(
    subdag=get_load_dim_dag(
        parent_dag_name=dag_name,
        task_id=load_songs_task_id,
        conn_id="redshift",
        table="songs",
        append=False,
        create_sql=sql_tables.song_table_create,
        load_sql=SqlQueries.song_table_insert,
        default_args=default_args
    ),
    task_id=load_songs_task_id,
    dag=dag
)

load_artists_task_id = "Load_artist_dim_table"
load_artists_subdag_task = SubDagOperator(
    subdag=get_load_dim_dag(
        parent_dag_name=dag_name,
        task_id=load_artists_task_id,
        conn_id="redshift",
        table="artists",
        append=False,
        create_sql=sql_tables.artist_table_create,
        load_sql=SqlQueries.artist_table_insert,
        default_args=default_args
    ),
    task_id=load_artists_task_id,
    dag=dag
)


data_quality_task_id = "Run_data_quality_checks"
data_quality_subdag_task = SubDagOperator(
    subdag=get_data_quality_dag(
        parent_dag_name=dag_name,
        task_id=data_quality_task_id,
        conn_id="redshift",
        default_args=default_args
    ),
    task_id=data_quality_task_id,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task Dependencies
#
start_operator >> stage_events_subdag_task
start_operator >> stage_songs_subdag_task

stage_events_subdag_task >> load_songplays_subdag_task
stage_songs_subdag_task >> load_songplays_subdag_task

load_songplays_subdag_task >> load_users_subdag_task
load_songplays_subdag_task >> load_times_subdag_task
load_songplays_subdag_task >> load_songs_subdag_task
load_songplays_subdag_task >> load_artists_subdag_task

# stage_events_subdag_task >> load_users_subdag_task
# stage_events_subdag_task >> load_times_subdag_task
# stage_songs_subdag_task >> load_songs_subdag_task
# stage_songs_subdag_task >> load_artists_subdag_task

load_users_subdag_task >> data_quality_subdag_task
load_times_subdag_task >> data_quality_subdag_task
load_songs_subdag_task >> data_quality_subdag_task
load_artists_subdag_task >> data_quality_subdag_task

data_quality_subdag_task >> end_operator