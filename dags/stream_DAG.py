import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from data.kafka_producer import generate_stream

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='stream_DAG',
    default_args=args,
    schedule_interval='@hourly',
    catchup=False,
)

task1 = PythonOperator(
    task_id='generate_stream',
    python_callable=generate_stream(),
    dag=dag
)
