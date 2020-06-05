import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import DataQualityOperator
#
# SubDAG runs data quality checks fo valid data
# on all the tables
#

# Test Callables
def test_func(db, sql, expected):
    records = db.get_records(sql)
    if not records or len(records) < 1 or len(records[0]) < 1:
        return False
    logging.info("SQL: "+sql)
    logging.info("expected: "+expected)
    logging.info("actual: "+str(records[0][0]))
    return records[0][0] == int(expected)


# Returns a DAG which runs various tests on various tables. 
def get_data_quality_dag(
        parent_dag_name,
        task_id,
        conn_id,
        tests,
        *args, **kwargs):
    
    # inherit DAG parameters
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    logging.info("Run all Tests")
    
    # Run all Tests
    for test in tests:
        test_task = DataQualityOperator(
            task_id=test['id'],
            dag=dag,
            conn_id=conn_id,
            test_sql=test['test_sql'],
            expected_result=test['expected_result'],
            test_callable=test_func,
            test_name=test['id']
        )

    # Tests can run in parallel without task dependencies
    return dag
