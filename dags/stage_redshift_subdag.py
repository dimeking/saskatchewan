import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import StageToRedshiftOperator

# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table. 
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
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    sql_drop_table = f"DROP TABLE IF EXISTS {table};" if not append else ""
    sql_create_table = create_sql.format(sql_drop_table, table) 
    create_task = PostgresOperator(
        task_id=f"create_{table}",
        dag=dag,
        postgres_conn_id=conn_id,
        sql=sql_create_table
    )

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

    create_task >> copy_task

    return dag
