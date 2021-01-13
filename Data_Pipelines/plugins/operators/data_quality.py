from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List
import operator

ops = {
    '>' : operator.ge   
    }

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables: List[str]=[""],
                 sql_check={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.sql_check = sql_check

    def execute(self, context):
        self.log.info('Creating redshift hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Redshift hook is created!')
        
        for table in self.tables:
            self.log.info(f'Running data quality check on table {table}')
            
            self.log.info(f'Getting number of entries in table {table}')
            records = redshift_hook.get_records(self.sql_check['test_sql'].format(table))
            self.log.info(f'Table {table} has {records} numbers of entries.')
            
            if not records or len(records[0]) < 1 or not ops[self.sql_check['comparison']](records[0][0], self.sql_check['expected_result']):
                self.log.error(f"Data quality check failed for table {table}")
                raise ValueError(f"Data quality check failed for table {table}")
                
            self.log.info(f"Data quality check passed for table {table}")