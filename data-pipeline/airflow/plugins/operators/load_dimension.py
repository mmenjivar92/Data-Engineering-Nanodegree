from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_query = """
                   INSERT INTO {}
                   (
                    {}
                   )
                   """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table ="",
                 query = "",
                 append_flag=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.query=query
        self.append_flag=append_flag

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_flag:
            self.log.info("Loading data into dim (append-only)")
            redshift.run(LoadDimensionOperator.insert_query.format(self.table,self.query))
        else:
            self.log.info("Loading data into dim (delete-load)")
            redshift.run("DELETE FROM {}".format(self.table))
            redshift.run(LoadDimensionOperator.insert_query.format(self.table,self.query))
        
        
