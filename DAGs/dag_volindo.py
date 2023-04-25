import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from datetime import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _save_posts(ti) -> None:
    """
    Save posts in specific location.
    :param ti: used to pull/push retrieved API data saved in XCOM's Airflow
    :return: file with name defined saved in 'tmp_weather_json_location' configured in Admin -> Variables Airflow UI.
    """
    # Pulls the return_value XCOM from "get_posts"
    posts = ti.xcom_pull(task_ids=['get_posts'])
    with open(Variable.get('tmp_weather_json_location'), 'w') as f:
        json.dump(posts[0], f)


def _process_weather_data():
    """
    Transformation's Process of the data retrieved.
    :return: file with name defined saved in 'tmp_weather_json_processed_location' configured in Admin -> Variables
             Airflow UI.
    """
    # weather = ti.xcom_pull(task_ids=['save_gets'])
    weather = Variable.get('tmp_weather_json_location')

    if not weather:
        raise Exception('No data.')

    data = json.load(open(weather))
    df = pd.json_normalize(data)

    # Creating an empty DF to poblate with data
    df_clean = pd.DataFrame()

    # Defining the columns to work with
    new_columns_name = ['temperature', 'humidity', 'wind_speed']
    columns_to_rename = ['main.temp', 'main.humidity', 'wind.speed']

    # Poblate new DF with API data and renaming columns
    df_clean[new_columns_name] = df[columns_to_rename]
    # Remember config correct Variables in UI
    df_clean.to_json(Variable.get('tmp_weather_json_processed_location'), orient='records')


def _upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    """
    Function to upload a .json file processed in an Amazon's bucket.
    :param filename: file's location to upload
    :param key: name defined for the file to upload
    :param bucket_name: Amazon's bucket name
    """
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    dag_id='api_dag',
    schedule_interval="@daily",
    start_date=datetime(2023, 4, 20),
    catchup=False
) as dag:

    # 1. Task to check for API active
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_weather',  # Remember config correct Connection_id in Admin > Connections
        endpoint='weather?q=Israel&appid=secretid&units=metric',
        email_on_failure=True,
        email='oscar.sanchezdw@gmail.com'
    )

    # 2. Task to retrieve data from API
    task_get_posts = SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_weather',
        endpoint='weather?q=Israel&appid=ba5831e9252f09fd13e6d1f1da96d1e2&units=metric',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        email_on_failure=True,
        email='oscar.sanchezdw@gmail.com'
    )

    # 3. Task to save the retrieved data
    task_save = PythonOperator(
        task_id='save_gets',
        python_callable=_save_posts,
        email_on_failure=True,
        email='oscar.sanchezdw@gmail.com'
    )

    # 4. Task for process the weather data
    task_process_weather_data = PythonOperator(
        task_id='process_weather_data',
        python_callable=_process_weather_data,
        email_on_failure=True,
        email='oscar.sanchezdw@gmail.com'
    )

    # 5. Upload file to S3
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=_upload_to_s3,
        op_kwargs={
            'filename': Variable.get('tmp_weather_json_processed_location'),
            'key': "weather_data/{{ds}}/weather_processed.json",
            'bucket_name': 'volindobucket'
        },
        email_on_failure=True,
        email='oscar.sanchezdw@gmail.com'
    )

    task_is_api_active >> task_get_posts >> task_save >> task_process_weather_data >> task_upload_to_s3
