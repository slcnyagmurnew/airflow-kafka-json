import json
import logging

from kafka import KafkaConsumer, TopicPartition
from json import loads
import pickle
import os
import time


def decode_json(file):
    barcodes = []
    amounts = []
    with open(file) as data:
        sample = json.load(data)
        file.close()
    for j in sample['completedCounts'][0]['contents']:
        barcode = j['barcode']
        amount = j['amount']
        barcodes.append(barcode)
        amounts.append(amount)
        logging.info('New barcode was added with amount')

    return barcodes, amounts


def data_from_kafka(**kwargs):

    consumer = KafkaConsumer(
        kwargs['topic'],
        bootstrap_servers=[kwargs['client']],
        consumer_timeout_ms=3000,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    logging.info('Consumer created')

    pickle.dump(barcodes, open(os.getcwd()+kwargs['path_new_data']+str(time.strftime("%Y%m%d_%H%M"))+"_new_samples.p", "wb"))
