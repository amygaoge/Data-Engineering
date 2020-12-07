from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info('Get credentials')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        #here is the logic allows to switch between truncate insert or just insert
        if self.truncate_insert:
            self.log.info(f'Truncate table {self.table}')
            truncate_sql = f"TRUNCATE TABLE {self.table}"
            redshift_hook.run(truncate_sql)
        
        self.log.info(f'Load dimension table {self.table} into Redshift')
        insert_sql = f"INSERT INTO {self.table} {self.select_sql}"
        redshift_hook.run(insert_sql)