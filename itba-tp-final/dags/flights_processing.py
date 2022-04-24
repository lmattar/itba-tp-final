import json
from airflow.models import DAG
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import   PythonOperator
import pandas as pd
import sys



def _printPath():
    print(sys.path)


default_args = {
    'start_date' : datetime(2022,3,15)
}

with DAG('flights-processing-dag'
        , schedule_interval='@daily'
        , default_args=default_args
        , catchup=False
    
        ) as dag:
    
    
    create_dep_delay_mean_table = PostgresOperator(
        task_id="create_agg_tables",
        postgres_conn_id='postgres_target',
        sql="""
            CREATE TABLE IF NOT EXISTS dep_delay_mean (
            ORIGIN VARCHAR ,
            FL_DATE DATE NOT NULL,
            DEP_DELAY_MEAN numeric NOT NULL,
            PRIMARY KEY (ORIGIN,FL_DATE,DEP_DELAY_MEAN)
            );
            CREATE TABLE IF NOT EXISTS flights_per_day (
            ORIGIN VARCHAR ,
            FL_DATE DATE NOT NULL,
            FL_COUNT numeric NOT NULL,
            OUTLIERS numeric ,
            PRIMARY KEY (ORIGIN,FL_DATE)
            );
          """,
    )
     
     
    
    sensor = S3KeySensor(
            task_id='check_s3_for_file_in_s3',
            bucket_key='tp/20*',
            wildcard_match= True,
            bucket_name='lmattar-itba-tp-final',
            aws_conn_id ='conn_s3_lab',
            timeout=18*60*60,
            poke_interval=10,
        dag=dag)
    
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/_api/airflow/providers/amazon/aws/operators/s3_list/index.html#airflow.providers.amazon.aws.operators.s3_list.S3ListOperator
    # https://stackoverflow.com/questions/67289076/apache-airflow-s3listoperator-not-listing-files
    # el resultado lo guarda en xcom en la base datos de airflow
    s3_files = S3ListOperator(
        task_id='list_3s_files',
        bucket='lmattar-itba-tp-final',
        prefix='tp/',
        delimiter='/',
        aws_conn_id='conn_s3_lab'
        )   

    #user templating para generar  varios dags
    # transform_file_over = S3FileTransformOperator(
    #         task_id='transform_file_over',
    #         source_s3_key='s3://lmattar-itba-tp-final/2009short.csv',
    #         dest_s3_key='s3://lmattar-itba-tp-final/2009mean.csv',
    #         transform_script= '/opt/airflow/dags/scripts/transformer.py', #how to call the python function
    #         replace= True,
    #         source_aws_conn_id='conn_s3_lab',
    #         dest_aws_conn_id='conn_s3_lab'
    #     )

    years = ["2009","2010","2011"]
    get_data_task = {}
    get_insert_task={}
    get_data_task_1 ={}
    get_insert_task_1={}
    get_update_task ={}
    for year in years:
          get_data_task[year] = S3FileTransformOperator(
                                    task_id=f"transform_file_over_{year}",
                                    source_s3_key=f's3://lmattar-itba-tp-final/tp/{year}.csv',
                                    dest_s3_key=f's3://lmattar-itba-tp-final/{year}mean.csv',
                                    transform_script= '/opt/airflow/dags/scripts/transformer.py', #how to call the python function
                                    replace= True,
                                    source_aws_conn_id='conn_s3_lab',
                                    dest_aws_conn_id='conn_s3_lab'
                                ) 
          get_insert_task[year] = PostgresOperator(
                task_id = f'insert_dep_delay_mean{year}'
                ,postgres_conn_id='postgres_target'
                ,sql=f'/sql/inserts_{year}.sql', 
            )

          get_data_task_1[year] = S3FileTransformOperator(
                                    task_id=f"transform_file_flight_per_day_{year}",
                                    source_s3_key=f's3://lmattar-itba-tp-final/tp/{year}.csv',
                                    dest_s3_key=f's3://lmattar-itba-tp-final/{year}_airport_flight_per_day.csv',
                                    transform_script= '/opt/airflow/dags/scripts/transformer_day.py', #how to call the python function
                                    replace= True,
                                    source_aws_conn_id='conn_s3_lab',
                                    dest_aws_conn_id='conn_s3_lab'
                                ) 

          get_insert_task_1[year] = PostgresOperator(
                task_id = f'insert_flihgt_per_day_{year}'
                ,postgres_conn_id='postgres_target'
                ,sql=f'/sql/inserts_per_day_{year}.sql', 
            )
        
          get_update_task[year] = PostgresOperator(
                task_id = f'update_outliers_{year}'
                ,postgres_conn_id='postgres_target'
                ,sql=f'/sql/update_outliers_{year}.sql', 
            )



    create_dep_delay_mean_table>>sensor>>s3_files
    for year in years:
        upstream_task = s3_files
        task = get_data_task[year]
        task1 =  get_data_task_1[year]
        upstream_task.set_downstream(task)
        upstream_task.set_downstream(task1)
        task.set_downstream(get_insert_task[year] )
        task1.set_downstream(get_insert_task_1[year])
        task2 = get_insert_task_1[year]
        task2.set_downstream(get_update_task[year])


    # print_path = PythonOperator(
    #     task_id = 'print_path'
    #     ,python_callable= _printPath
    # )