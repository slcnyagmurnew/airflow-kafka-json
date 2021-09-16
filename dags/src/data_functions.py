import json
import logging
import time
from json import loads
import psycopg2

from kafka import KafkaConsumer

PGHOST = 'postgres'
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
        barcode = list(sample[i].keys())[0]
        amount = list(sample[i].values())[0]
        barcodes.append(barcode)
        amounts.append(amount)
    logging.info('New barcodes were added with amounts')

    return barcodes, amounts


def update_db(count, barcodes, amounts):

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()  # create cursor

    for i in range(count):
        try:
            sql = """INSERT INTO products (barcode, amount)
VALUES (%s, %s)
ON CONFLICT (barcode)
DO
    UPDATE SET amount = products.amount + %s"""
            record = (barcodes[i], int(amounts[i]), int(amounts[i]))
            cursor.execute(sql, record)

            conn.commit()
            print('Inserted successfully!')
        except (Exception, psycopg2.Error) as err:
            print(err)
        finally:
            print('Loop executed')
    if conn:
        cursor.close()
        conn.close()
        print('Connection closed')


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

        # logging.info("Offset: ", item.offset)
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

