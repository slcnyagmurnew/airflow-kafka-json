import json
import logging
import time
from json import loads

from kafka import KafkaConsumer
from json_functions import update_db

PGHOST = 'localhost'
PGDATABASE = 'airflow'
PGUSER = 'airflow'
PGPASSWORD = 'airflow'

conn_string = "host=" + PGHOST + " port=" + "5432" + " dbname=" + PGDATABASE + " user=" + PGUSER \
              + " password=" + PGPASSWORD


def decode_json(data):
    barcodes = []
    amounts = []

    sample = json.loads(data)
    for i in range(len(sample)):
        barcode = sample[i]['barcode']
        amount = sample[i]['amount']
        barcodes.append(barcode)
        amounts.append(amount)
    logging.info('New barcodes were added with amounts')

    return barcodes, amounts


def data_from_kafka(**kwargs):
    """

    :param kwargs:
    :return:
    """

    consumer = KafkaConsumer(
        kwargs['topic'],
        bootstrap_servers=[kwargs['client']],
        consumer_timeout_ms=3000,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    logging.info('Consumer created')

    products = []

    for item in consumer:  # item is not file name, json object

        logging.info("Offset: ", item.offset)
        item = item.value
        barcodes, amounts = decode_json(item)  # barcodes and amounts from one json file
        count = len(barcodes)

        for i in range(count):
            product = {
                "barcode": barcodes[i],
                "amount": amounts[i]
            }
            products.append(product)

        update_db(count, barcodes, amounts)

    with open('/usr/local/airflow/data/logs/log-' + str(time.strftime("%Y%m%d_%H%M")) + '.json', 'w') as outfile:
        json.dump(products, outfile)
        outfile.close()
