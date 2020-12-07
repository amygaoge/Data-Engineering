from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info('Get credentials')       
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f'Load fact table {self.table} into Redshift')
        insert_sql= f"INSERT INTO {self.table} {self.select_sql}"
        redshift_hook.run(insert_sql)