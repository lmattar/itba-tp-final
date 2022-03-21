import json
from airflow.models import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import   PythonOperator


from pandas import json_normalize  

def _processing_user(ti):
    users=ti.xcom_pull(task_ids=['extracting_user'])

    if not len(users) or 'results' not in users[0]:
        ValueError('User is empty')

    user = users[0]['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first']
        ,'lastname': user['name']['last']
        , 'country':user['location']['country']
        , 'username':user['login']['username']
        , 'password':user['login']['password']
        , 'email': user['email']
             } )

    processed_user.to_csv('/tmp/processed_user.csv',index=None,header=False)
    with open('/opt/airflow/dags/sql/inserts.sql','w'):
        for index, row in processed_user.iterrows():
            print('INSERT INTO users(' + 
                            'username' + 
                            ',firstname' +
                            ',lastname' +
                            ',country' +
                            ',password)' +
                            'VALUES(' + '\'' +row['username']+ '\'' +
                            ',\'' +row['firstname']+ '\'' +
                            ',\'' +row['lastname']+ '\'' +
                            ',\'' +row['country']+ '\'' +
                            ',\'' +row['password']+ '\'' +
                            ');',file=open('/opt/airflow/dags/sql/inserts.sql','a')) 
        


default_args = {
    'start_date' : datetime(2022,3,15)
}

with DAG('user-dag'
        , schedule_interval='@daily'
        , default_args=default_args
        , catchup=False
        ) as dag:

    create_user_table = PostgresOperator(
        task_id="create_user_table",
        postgres_conn_id='postgres_target',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
            username VARCHAR PRIMARY KEY,
            firstname VARCHAR NOT NULL,
            lastname VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            password VARCHAR NOT NULL);
          """,
    )

    is_api_available = HttpSensor(
        task_id = 'is_api_available'
        ,http_conn_id='http_conn'
        , endpoint='api/'
    )

    extracting_user = SimpleHttpOperator(
        task_id = 'extracting_user'
        ,http_conn_id='http_conn'
        ,endpoint='/api'
        ,method='GET'
        ,response_filter= lambda response: json.loads(response.text)
        ,log_response=True
    )

    processing_user = PythonOperator(
        task_id = 'processing_user'
        ,python_callable= _processing_user
    )

    storing_user = PostgresOperator(
        task_id = 'storing_user'
        ,postgres_conn_id='postgres_target'
        ,sql='/sql/inserts.sql', 
    )
  

    create_user_table >> is_api_available >> extracting_user >> processing_user >> storing_user
