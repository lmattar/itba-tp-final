import json
from airflow.models import DAG
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator 

default_args = {
    'start_date' : datetime(2022,3,15)
}

with DAG('flights-processing-dag'
        , schedule_interval='@daily'
        , default_args=default_args
        , catchup=False
    
        ) as dag:

    sensor = S3KeySensor(
            task_id='check_s3_for_file_in_s3',
            bucket_key='tp/10*',
            wildcard_match= True,
            bucket_name='lmattar-itba-cde-tp-final',
            aws_conn_id ='conn_s3_lab',
            timeout=18*60*60,
            poke_interval=10,
        dag=dag)
    
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/_api/airflow/providers/amazon/aws/operators/s3_list/index.html#airflow.providers.amazon.aws.operators.s3_list.S3ListOperator
    # https://stackoverflow.com/questions/67289076/apache-airflow-s3listoperator-not-listing-files
    # el resultado lo guarda en xcom en la base datos de airflow
    s3_files = S3ListOperator(
        task_id='list_3s_files',
        bucket='lmattar-itba-cde-tp-final',
        prefix='tp/',
        delimiter='/',
        aws_conn_id='conn_s3_lab'
        )   

    transform_file_over = S3FileTransformOperator(
            task_id='transform_file_over',
            source_s3_key='s3://lmattar-itba-cde-tp-final/s3Wells.csv',
            dest_s3_key='s3://lmattar-itba-cde-tp-final/S3test5.txt',
            transform_script= '/opt/airflow/dags/scripts/transformer.py', #how to call the python function
            source_aws_conn_id='conn_s3_lab',
            dest_aws_conn_id='conn_s3_lab'
        )