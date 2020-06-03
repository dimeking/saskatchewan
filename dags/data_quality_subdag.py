import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import DataQualityOperator

# Test Callables
def test_has_rows(db, table):
    records = db.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        return False
    return records[0][0] > 0


# Returns a DAG which runs various tests on various tables. 
def get_data_quality_dag(
        parent_dag_name,
        task_id,
        conn_id,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # Test 'Has Rows' on staging_events_table 
    staging_events_test_has_rows_task = DataQualityOperator(
        task_id='staging_events_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='staging_events_table'
    )
        
    # Test 'Has Rows' on staging_songs_table 
    staging_songs_test_has_rows_task = DataQualityOperator(
        task_id='staging_songs_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='staging_songs_table'
    )
        
    # Test 'Has Rows' on songplays 
    songplays_test_has_rows_task = DataQualityOperator(
        task_id='songplays_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='songplays'
    )
        
    # Test 'Has Rows' on users table
    users_test_has_rows_task = DataQualityOperator(
        task_id='users_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='users'
    )
    
    # Test 'Has Rows' on times table
    times_test_has_rows_task = DataQualityOperator(
        task_id='times_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='times'
    )

    # Test 'Has Rows' on songs table
    songs_test_has_rows_task = DataQualityOperator(
        task_id='songs_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='songs'
    )
    
    # Test 'Has Rows' on artists table
    artists_test_has_rows_task = DataQualityOperator(
        task_id='artists_test_has_rows_task',
        dag=dag,
        conn_id=conn_id,
        test_name='Has Rows',
        test_callable=test_has_rows,
        expected_result=True,
        table='artists'
    )

    return dag
