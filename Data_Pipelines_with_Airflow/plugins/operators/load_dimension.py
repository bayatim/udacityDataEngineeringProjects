from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 clear_content=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.clear_content = clear_content

    def execute(self, context):
        self.log.info('Creating redshift hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Redshift hook is created!')
        
        if self.clear_content:
            self.log.info(f"Clearing data from dimention table {self.table}")
            redshift_hook.run(f"DELETE FROM {self.table}")
            self.log.info(f'Dimention table {self.table} is emptied!')
        
        self.log.info('Start loading dimention table {self.table}')
        redshift_hook.run(self.sql_query)
        self.log.info(f"Dimention table {self.table} loaded!")
