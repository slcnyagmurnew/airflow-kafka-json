import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.json_functions import divide_jsons, create_table, add_to_db

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': False,
}

dag = DAG(
    dag_id='initial_DAG',
    schedule_interval='@once',
    default_args=args,
    catchup=False
)

task1 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

task2 = PythonOperator(
    task_id='add_to_db',
    python_callable=add_to_db,
    dag=dag
)

task3 = PythonOperator(
    task_id='divide_jsons',
    python_callable=divide_jsons,
    dag=dag
)

task1 >> task2 >> task3
