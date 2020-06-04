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
                 table="",
                 test_name="",
                 test_callable=None,
                 expected_result=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.test_name = test_name
        self.test_callable = test_callable
        self.expected_result = expected_result


    def execute(self, context):
        # acquire hook
        db = PostgresHook(postgres_conn_id=self.conn_id)
        
        # check test params & input data 
        if self.table=="":
            logging.info("No Table to test {self.test_name}")
            
        if not self.test_callable or not self.expected_result:
            logging.info(f"Test call or expected result for {self.test_name} unavailable")
        
        # Run test and Check result
        # Raise error or Log success 
        result = self.test_callable(db, self.table)
        if result is not self.expected_result:
            raise ValueError(f"Data quality check {self.test_name} on table {self.table} failed.")
        
        logging.info(f"Data quality check {self.test_name} on table {self.table} passed.")
