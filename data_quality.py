from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 list_of_tables = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_of_tables = list_of_tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('DataQualityOperator in progress: checking data quality for all tables...')

        for table in self.list_of_tables:
            self.log.info(f'DataQualityOperator in progress: checking data quality for {table} table')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        self.log.info('DataQualityOperator successfully implemented: quality of all tables have been checked')
