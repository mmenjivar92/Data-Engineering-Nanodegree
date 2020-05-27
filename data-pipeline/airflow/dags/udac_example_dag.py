from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

dq_checks=[
            #Load Check and Primary Key check for artists
            {'check_sql':"SELECT COUNT(*) FROM artists", 'expected_result':0, 'condition':'greater_than'},
            {'check_sql':"SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0, 'condition':'equal'},
            #Load Check and Primary Key check for users
            {'check_sql':"SELECT COUNT(*) FROM users", 'expected_result':0, 'condition':'greater_than'},
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0 , 'condition': 'equal'},
            #Load Check and Primary check for songs
            {'check_sql':"SELECT COUNT(*) FROM songs", 'expected_result':0, 'condition':'greater_than'},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0, 'condition': 'equal'},
            #Load Check and Primary check for time
            {'check_sql':"SELECT COUNT(*) FROM time", 'expected_result':0, 'condition':'greater_than'},
            {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0, 'condition': 'equal'},
            #Load check on songplays
            {'check_sql':"SELECT COUNT(*) FROM songplays", 'expected_result':0, 'condition':'greater_than'}
          ]

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=True,
          max_active_runs= 1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{{execution_date.year}}/{{execution_date.month}}/",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data" 
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    query=SqlQueries.user_table_insert,
    append_flag=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    query=SqlQueries.song_table_insert,
    append_flag=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    query=SqlQueries.artist_table_insert,
    append_flag=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    query=SqlQueries.time_table_insert,
    append_flag=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Dependencies
stage_events_to_redshift.set_upstream([start_operator])
stage_songs_to_redshift.set_upstream([start_operator])
load_songplays_table.set_upstream([stage_events_to_redshift,stage_songs_to_redshift])
load_user_dimension_table.set_upstream([load_songplays_table])
load_song_dimension_table.set_upstream([load_songplays_table])
load_artist_dimension_table.set_upstream([load_songplays_table])
load_time_dimension_table.set_upstream([load_songplays_table])
run_quality_checks.set_upstream([load_user_dimension_table,
                                 load_song_dimension_table,
                                 load_artist_dimension_table,
                                 load_time_dimension_table])
end_operator.set_upstream([run_quality_checks])

