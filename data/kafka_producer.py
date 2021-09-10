from json import dumps
import json
import os
import logging
from time import sleep

from kafka import KafkaProducer


def encode_to_json(file):
    """
    After reading initial json file, remaining files are divided and encoded to json
    :param file: Randomly selected file that will be dumped to json
    :return: Dumped json data
    """
    with open(file) as our_file:
        json_file = json.load(our_file)
        our_file.close()
    our_list = []
    for content in json_file['completedCounts'][0]['contents']:
        our_list.append({content['barcode']: content['amount']})
    dumped = dumps(our_list)
    return dumped


def generate_stream():
    """
    Sends json data to kafka broker
    :return: None
    """
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],  # set up Producer
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    path = os.listdir(os.getcwd() + '/jsons')[0]  # json name
    full_path = os.getcwd() + '/jsons/' + path  # json path
    products = encode_to_json(full_path)

    logging.info('Partitions: ', producer.partitions_for('Topic1'))
    producer.send('Topic1', value=products)
    sleep(3)

    producer.close()
