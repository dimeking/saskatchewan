import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import LoadDimensionOperator

# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table. 
def get_load_dim_dag(
        parent_dag_name,
        task_id,
        conn_id,
        table,
        append,
        create_sql,
        load_sql,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    sql_drop_table = f"DROP TABLE IF EXISTS {table};" if not append else ""
    sql_create_table = create_sql.format(sql_drop_table, table) 
    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=conn_id,
        sql=sql_create_table
    )

    load_task = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        conn_id=conn_id,
        table=table,
        append=append,
        sql=load_sql
    )


    create_task >> load_task

    return dag
