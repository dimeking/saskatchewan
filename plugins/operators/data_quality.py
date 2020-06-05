import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#
# Operator runs a specific test (data quality check) 
# on a specified table
#
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 test_sql="",
                 expected_result=None,
                 test_callable=None,
                 test_name="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.test_sql = test_sql
        self.expected_result = expected_result
        self.test_callable = test_callable
        self.test_name = test_name


    def execute(self, context):
        # acquire hook
        db = PostgresHook(postgres_conn_id=self.conn_id)
        
        # check test params & input data 
        if self.test_sql=="":
            logging.info(f"No SQL for {self.test_name} test")
            
        if not self.test_callable or not self.expected_result:
            logging.info(f"Test call or expected result for {self.test_name} unavailable")
        
        # Run test and Check result
        # Raise error or Log success 
        result = self.test_callable(db, self.test_sql, self.expected_result)
        if not result:
            raise ValueError(f"Data quality check {self.test_name} failed.")
        
        logging.info(f"Data quality check {self.test_name} passed.")
