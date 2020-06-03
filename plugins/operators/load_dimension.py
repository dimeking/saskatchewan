from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 append=False,
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.append = append
        self.sql = sql

    def execute(self, context):

        db = PostgresHook(postgres_conn_id=self.conn_id)

        if not self.append:
            self.log.info("Clearing data from destination table")
            db.run("DELETE FROM {}".format(self.table))
        
        formatted_sql = self.sql.format(self.table)
        self.log.info("formatted_sql")
        db.run(formatted_sql)
