from json import dumps
import os
import logging
from time import sleep

from kafka import KafkaProducer
from json_functions import encode_to_json


def generate_stream():
    """
    Sends json src to kafka broker
    :return: None
    """
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],  # set up Producer
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    path = os.listdir('/usr/local/airflow/data/jsons')[0]  # json name
    full_path = '/usr/local/airflow/data/jsons/' + path  # json path
    products = encode_to_json(full_path)

    logging.info('Partitions: ', producer.partitions_for('Topic1'))
    producer.send('Topic1', value=products)
    sleep(3)

    producer.close()
