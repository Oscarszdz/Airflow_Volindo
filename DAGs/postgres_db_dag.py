import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

def _get_iris_data():
    sql_stmt = "SELECT * FROM iris"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='airflow'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt),
    return cursor.fetchall()


def _process_iris_data(ti):
    iris = ti.xcom_pull(task_ids=['get_iris_data'])
    if not iris:
        raise Exception('No data.')

    iris = pd.DataFrame(
        data=iris[0],
        columns=['iris_id', 'iris_sepal_length', 'iris_sepal_width',
                 'iris_petal_length', 'iris_petal_width', 'iris_variety']
    )
    # ETL process filter criteria
    iris = iris[
        (iris['iris_sepal_length'] > 5) &
        (iris['iris_sepal_width'] == 3) &
        (iris['iris_petal_length'] > 3) &
        (iris['iris_petal_width'] == 1.5)
    ]
    iris = iris.drop('iris_id', axis=1)
    iris.to_csv(Variable.get('tmp_iris_csv_location'), index=False)


def _truncate_tgt_table():
    sql_truncate = "TRUNCATE TABLE iris_tgt"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='airflow'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_truncate),
    # return cursor.fetchall()


with DAG(
    dag_id='postgres_db_dag',
    # schedule_interval='@daily',
    schedule_interval='*/1 * * * *',
    start_date=datetime(year=2023, month=4, day=20),
    catchup=False
) as dag:

    # 1. Get the Iris data from a table in Postgres
    task_get_iris_data = PythonOperator(
        task_id='get_iris_data',
        python_callable=_get_iris_data,
        do_xcom_push=True
    )

    # 2. Process the Iris data
    task_process_iris_data = PythonOperator(
        task_id='process_iris_data',
        python_callable=_process_iris_data
    )

    # 3. Truncate table in Postgres
    task_truncate_tgt_table = PythonOperator(
        task_id='truncate_tgt_table',
        python_callable=_truncate_tgt_table
        )

    # 4. Save to Postgres
    task_load_iris_data = BashOperator(
        task_id='load_iris_data',
        bash_command=(
            'psql -d airflow -U postgres -c "'
            'COPY iris_tgt(iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety) '
            "FROM '/tmp/iris_processed.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )


task_get_iris_data >> task_process_iris_data >> task_truncate_tgt_table >> task_load_iris_data
