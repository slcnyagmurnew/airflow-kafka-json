import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.data_functions import data_from_kafka

CLIENT = 'kafka:9092'
TOPIC = 'Topic1'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': False,
}

dag = DAG(
    dag_id='update_DAG',
    schedule_interval='@daily',
    default_args=args,
    catchup=False
)

task1 = PythonOperator(
    task_id='data_from_kafka',
    python_callable=data_from_kafka,
    op_kwargs={'client': CLIENT,
               'topic': TOPIC},
    dag=dag
)
