import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import LoadFactOperator

#
# SubDAG creates songplays facts table (if need be) 
# from staging_songs_table & staging_events_table
#
def get_load_fact_dag(
        parent_dag_name,
        task_id,
        conn_id,
        table,
        append,
        create_sql,
        load_sql,
        *args, **kwargs):

    # inherit DAG parameters    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    action = 'Append data to' if append else 'Populate data in'
    logging.info(f"{action} {table} fact table")

    # Drop Table if append mode is not enabled
    # Create Table on Postgres Redshift with connection id from airflow
    sql_drop_table = f"DROP TABLE IF EXISTS {table};" if not append else ""
    sql_create_table = create_sql.format(sql_drop_table, table) 
    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=conn_id,
        sql=sql_create_table
    )

    # Enable Load Fact Operator to create fact tables from staging tables
    load_task = LoadFactOperator(
        task_id=f"load_{table}_fact_table",
        dag=dag,
        conn_id=conn_id,
        table=table,
        append=append,
        sql=load_sql
    )

    # ensure load task is executed after create task
    create_task >> load_task

    return dag
