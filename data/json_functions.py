import json
import argparse
import os
import psycopg2
from json import dumps

parser = argparse.ArgumentParser()
parser.add_argument('--init', help='Initial integer value for first json',
                    default=26)
parser.add_argument('--file', help='JSON file name',
                    default=os.getcwd() + '/data/counts.json')
args = parser.parse_args()

INITIAL_COUNT = int(args.init)
FILE_NAME = args.file

PGHOST = 'localhost'
PGDATABASE = 'airflow'
PGUSER = 'airflow'
PGPASSWORD = 'airflow'

conn_string = "host=" + PGHOST + " port=" + "5432" + " dbname=" + PGDATABASE + " user=" + PGUSER \
              + " password=" + PGPASSWORD


def add_to_db(count=INITIAL_COUNT, file=FILE_NAME):
    """
    Parse barcode and amount of data in counts.json
    Write parsed data into postgre database.
    :param count: # of example data which will be written to db
    :param file: new json file name to write parsed data
    :return:
    """
    with open(file) as our_file:
        json_file = json.load(our_file)
        our_file.close()

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()  # create cursor

    for i in range(count):
        data = json_file[i]
        for j in data['completedCounts'][0]['contents']:

            try:
                sql = """ INSERT INTO products VALUES (%s,%s)"""
                record = (j['barcode'], int(j['amount']))
                cursor.execute(sql, record)

                conn.commit()
                print('Inserted successfully!')
            except (Exception, psycopg2.Error):
                sql = """ UPDATE products SET amount=amount + %d WHERE barcode=%s"""
                record = (int(j['amount']), j['barcode'])
                cursor.execute(sql, record)

                conn.commit()
                print('Updated successfully')
            finally:
                if conn:
                    cursor.close()
                    conn.close()
                    print('Connection closed')


def divide_jsons(file=FILE_NAME):
    """
    Divide into json files for stream data
    :param file: File (with path) to write new json files
    :return:
    """
    with open(file) as our_file:
        json_file = json.load(our_file)
        our_file.close()

    for i in range(INITIAL_COUNT, len(json_file)):
        data = json_file[i]
        with open('./data/jsons/data-' + str(i) + '.json', 'w') as outfile:
            json.dump(data, outfile)
            outfile.close()


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


def update_db(count, barcodes, amounts):

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()  # create cursor

    for i in range(count):
        try:
            sql = """ INSERT INTO products VALUES (%s, %s)"""
            record = (barcodes[i], int(amounts[i]))
            cursor.execute(sql, record)

            conn.commit()
            print('Inserted successfully!')
        except (Exception, psycopg2.Error):
            sql = """ UPDATE products SET amount=amount + %d WHERE barcode=%s"""
            record = (barcodes[i], int(amounts[i]))
            cursor.execute(sql, record)

            conn.commit()
            print('Updated successfully')
        finally:
            if conn:
                cursor.close()
                conn.close()
                print('Connection closed')
