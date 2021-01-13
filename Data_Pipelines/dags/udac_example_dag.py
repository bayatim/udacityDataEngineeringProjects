from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import CreateTableOperator
from airflow.operators import StageToRedshiftOperator 
from airflow.operators.udacity_plugin import LoadFactOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator
from airflow.operators.udacity_plugin import DataQualityOperator
                              
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
    'start_date': datetime(2021, 1, 13)
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 */1 * * *'
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_redshift_tables = CreateTableOperator(
        task_id="create_redshift_tables",
        dag=dag,
        redshift_conn_id="redshift",
        query_path='/home/workspace/airflow/create_tables.sql'
    )


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    destination_table="staging_events",
    json_format="s3://udacity-dend/log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    destination_table="staging_songs",
    json_format="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    table="users",
    clear_content=True
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    table="songs",
    clear_content=True
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    table="artists",
    clear_content=True
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    table="time",
    clear_content=True
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "users","songs","artists","time"],
    sql_check= SqlQueries.test_table_entries
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_redshift_tables >> (stage_events_to_redshift,
                                             stage_songs_to_redshift) >> load_songplays_table 
load_songplays_table >> (load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table) >> run_quality_checks >> end_operator