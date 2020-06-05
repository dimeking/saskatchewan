from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from stage_redshift_subdag import get_stage_redshift_dag
from load_dim_subdag import get_load_dim_dag
from load_fact_subdag import get_load_fact_dag
from data_quality_subdag import get_data_quality_dag

from helpers import SqlQueries
from helpers import sql_tables


# Connections aws_credentials & redshift are setup 
# on AirFlow Admin Connections page and accessed via Hooks
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

"""
SETUP DAG
    Configure DAG's with default paramters:
    - The DAG does not have dependencies on past runs
    - On failure, the task are retried 3 times
    - Retries happen every 5 minutes
    - Catchup is turned off
    - Do not email on retry
    - hourly schedule
"""
logging.info("SETUP DAG")

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

# dummy start operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# SubDAG creates staging_events_table (if need be) and copies 
# JSON files from  s3://udacity-dend/log_data to Amazon Redshift
#
logging.info("Copy json files from S3 (s3://udacity-dend/log_data) to staging_events_table on Redshift ")
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

#
# SubDAG creates staging_songs_table (if need be) and copies 
# JSON files from  s3://udacity-dend/song_data to Amazon Redshift
#
logging.info("Copy json files from S3 (s3://udacity-dend/song_data) to staging_songs_table on Redshift ")
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

#
# SubDAG creates songplays facts table (if need be) 
# from staging_songs_table & staging_events_table
#
logging.info("")
logging.info("Populate songplays fact table on Redshift from staging stables")
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

#
# SubDAG creates users dimension table (if need be) 
# from staging_events_table
#
logging.info("Populate users dimension table on Redshift from staging stables")
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

#
# SubDAG creates times dimension table (if need be) 
# from staging_events_table
#
logging.info("Populate times dimension table on Redshift from staging stables")
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

#
# SubDAG creates songs dimension table (if need be) 
# from staging_songs_table
#
logging.info("Populate songs dimension table on Redshift from staging stables")
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

#
# SubDAG creates artists dimension table (if need be) 
# from staging_artists_table
#
logging.info("Populate artists dimension table on Redshift from staging stables")
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

#
# SubDAG runs data quality checks for valid data
# on all the tables
#
tests = [
        {"test_sql":" SELECT COUNT(*) from staging_events_table" , "expected_result":"8056", "id":"staging_events_table_count"} , 
        {"test_sql":" SELECT COUNT(*) from staging_songs_table" , "expected_result":"14896", "id":"staging_songs_table_count"} , 
        {"test_sql":" SELECT COUNT(*) from songplays" , "expected_result":"320", "id":"songplays_table_count"},
        {"test_sql":" SELECT COUNT(*) from users" , "expected_result":"104", "id":"users_table_count"},
        {"test_sql":" SELECT COUNT(*) from times" , "expected_result":"6813", "id":"times_table_count"},
        {"test_sql":" SELECT COUNT(*) from songs" , "expected_result":"14896", "id":"songs_table_count"},
        {"test_sql":" SELECT COUNT(*) from artists" , "expected_result":"10025", "id":"artists_table_count"}
    ]

logging.info("")
logging.info("Run data quality checks over staging, fact & dimension tables ")
data_quality_task_id = "Run_data_quality_checks"
data_quality_subdag_task = SubDagOperator(
    subdag=get_data_quality_dag(
        parent_dag_name=dag_name,
        task_id=data_quality_task_id,
        conn_id="redshift",
        tests=tests,
        default_args=default_args
    ),
    task_id=data_quality_task_id,
    dag=dag
)

# dummy end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task Dependencies
#
logging.info("")
logging.info("Begin_execution >> Stage_events >> Load_songplays_fact_table >> Load_user_dim_table >> Run_data_quality_checks >> Stop_execution")
logging.info("Begin_execution >> Stage_events >> Load_songplays_fact_table >> Load_time_dim_table >> Run_data_quality_checks >> Stop_execution")
logging.info("Begin_execution >> Stage_songs >> Load_songplays_fact_table >> Load_song_dim_table >> Run_data_quality_checks >> Stop_execution")
logging.info("Begin_execution >> Stage_songs >> Load_songplays_fact_table >> Load_artist_dim_table >> Run_data_quality_checks >> Stop_execution")

# kickoff by creating staging stables from S3 buckets
start_operator >> stage_events_subdag_task
start_operator >> stage_songs_subdag_task

# load songplays facts table by joining events & songs staging tables
stage_events_subdag_task >> load_songplays_subdag_task
stage_songs_subdag_task >> load_songplays_subdag_task

# load users, times, songs, artists dimension tables
load_songplays_subdag_task >> load_users_subdag_task
load_songplays_subdag_task >> load_times_subdag_task
load_songplays_subdag_task >> load_songs_subdag_task
load_songplays_subdag_task >> load_artists_subdag_task

# do data quality checks on all the tables
load_users_subdag_task >> data_quality_subdag_task
load_times_subdag_task >> data_quality_subdag_task
load_songs_subdag_task >> data_quality_subdag_task
load_artists_subdag_task >> data_quality_subdag_task

# end
data_quality_subdag_task >> end_operator