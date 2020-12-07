from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Get credentials')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data Quality Check failed. {table} returns no results.')
            
            self.log.info(f' Data Quality Check on table {table} passed with {records[0][0]} records.')