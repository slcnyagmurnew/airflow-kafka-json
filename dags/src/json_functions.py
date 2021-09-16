import json
import psycopg2

# parser = argparse.ArgumentParser()
# parser.add_argument('--init', help='Initial integer value for first json',
#                     default=26)
# parser.add_argument('--file', help='JSON file name',
#                     default='/usr/local/airflow/data/counts.json')
# args = parser.parse_args()
#
# INITIAL_COUNT = int(args.init)
# FILE_NAME = args.file
#
PGHOST = 'postgres'
PGDATABASE = 'airflow'
PGUSER = 'airflow'
PGPASSWORD = 'airflow'

conn_string = "host=" + PGHOST + " port=" + "5432" + " dbname=" + PGDATABASE + " user=" + PGUSER \
              + " password=" + PGPASSWORD

file = '/usr/local/airflow/data/counts.json'
count = 26


def add_to_db():
    """
    Parse barcode and amount of src in counts.json
    Write parsed src into postgre database.
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
                sql = """INSERT INTO products (barcode, amount)
VALUES (%s, %s)
ON CONFLICT (barcode)
DO
    UPDATE SET amount = products.amount + %s"""
                record = (j['barcode'], j['amount'], j['amount'])
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


def divide_jsons():
    """
    Divide into json files for stream src
    :return:
    """
    with open(file) as our_file:
        json_file = json.load(our_file)
        our_file.close()

    for i in range(count, len(json_file)):
        data = json_file[i]
        with open('/usr/local/airflow/data/jsons/src-' + str(i) + '.json', 'w') as outfile:
            json.dump(data, outfile)
            outfile.close()


def create_table():

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()  # create cursor

    try:
        sql = "CREATE TABLE IF NOT EXISTS products (barcode VARCHAR(11) PRIMARY KEY, amount INT);"
        cursor.execute(sql)

        conn.commit()
        print('Table created successfully!')
    except (Exception, psycopg2.Error) as err:
        print(err)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print('Connection closed')



