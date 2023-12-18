from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 dim_table_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.dim_table_query = dim_table_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'LoadDimensionOperator in progress: loading {self.table} into dim table')
        redshift.run("INSERT INTO {} {}".format(self.table, self.dim_table_query))
        
        self.log.info(f'LoadDimensionOperator successfully implemented: dim table {self.table} has been loaded')
