from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_path="",
                 *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_path = query_path 

    def execute(self, context):
        self.log.info("Creating redshift sql hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Readign query for creating table")
        create_table_query = open(self.query_path, 'r').read()
        
        self.log.info("creating table")
        redshift.run(create_table_query)
        self.log.info("Table is created")