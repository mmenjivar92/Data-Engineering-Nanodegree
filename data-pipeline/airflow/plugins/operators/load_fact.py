from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
                 INSERT INTO public.songplays(playid,
                                              start_time, 
                                              userid, 
                                              level, 
                                              songid, 
                                              artistid, 
                                              sessionid, 
                                              location, 
                                              user_agent)
                (
                {}
                )
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.query=query
        
    def execute(self, context):
        redshift_c = PostgresHook(self.redshift_conn_id)
        self.log.info("Loading data into fact table")
        redshift_c.run(LoadFactOperator.insert_sql.format(self.query))