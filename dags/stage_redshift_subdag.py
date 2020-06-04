import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import StageToRedshiftOperator

#
# SubDAG creates staging_events_table (if need be) and copies 
# JSON files from  S3 to Amazon Redshift
#
def get_stage_redshift_dag(
        parent_dag_name,
        task_id,
        conn_id,
        aws_credentials_id,
        table,
        append,
        create_sql,
        s3_bucket,
        s3_prefix,
        s3_json,
        *args, **kwargs):
    
    # inherit DAG parameters
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    action = 'Append data to' if append else 'Create data in'
    logging.info(f"{action} {table} table")

    # Drop Table if append mode is not enabled
    # Create Table on Postgres Redshift with connection id from airflow
    sql_drop_table = f"DROP TABLE IF EXISTS {table};" if not append else ""
    sql_create_table = create_sql.format(sql_drop_table, table) 
    create_task = PostgresOperator(
        task_id=f"create_{table}",
        dag=dag,
        postgres_conn_id=conn_id,
        sql=sql_create_table
    )

    # Enable S3 to Redshift Operator to copy json data
    copy_task = StageToRedshiftOperator(
        task_id=f"copy_{table}",
        dag=dag,
        redshift_conn_id=conn_id,
        aws_credentials_id=aws_credentials_id,
        table=table,
        append=append,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_json=s3_json
    )  

    # ensure copy task is executed after create task
    create_task >> copy_task

    return dag
