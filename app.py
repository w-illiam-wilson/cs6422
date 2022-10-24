from util import load_mysql_args, load_clickhouse_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP
from migrate_to_clickhouse import migrate_to_clickhouse
from add_rows import write_data
from threading import Thread
import time
from clickhouse_driver import Client
import mysql.connector

def periodic_migration(period):
    while True:
        time.sleep(period)
        migrate_to_clickhouse()

def main():
    print("Connecting to MySQL database...")
    mysql_conn = mysql.connector.connect(**load_mysql_args())
    mysql_cursor = mysql_conn.cursor()

    print("Init MySQL database...")
    fd = open('create_database.sql', 'r')
    create_sql_db_script = fd.read()
    fd.close()
    for command in create_sql_db_script.split(';\n'):
        # print(command)
        mysql_cursor.execute(command)
        mysql_conn.commit()

    print("Connecting to Clickhouse database...")
    clickhouse_client = Client(**load_clickhouse_args())

    print("Init Clickhouse database...")
    fd = open('create_database_clickhouse.sql', 'r')
    create_clickhouse_db_script = fd.read()
    fd.close()
    for command in create_clickhouse_db_script.split(';\n'):
        # print(command)
        clickhouse_client.execute(command)

    print("starting user threads, simulating adding data")
    # TODO write a client class which contains write_data() and migrate_to_clickhouse()
    # so that migrate_to_clickhouse() can block the write_data() when it is running
    # may or may not be necessary depending on if mysql.connector is thread safe.

    user_threads = []
    for stock_name in STOCK_TO_DATA_FILE_NAME_MAP.keys():
        conn = mysql.connector.connect(**load_mysql_args())
        cursor = conn.cursor()
        user_threads.append(Thread(target = write_data, args=(conn, cursor, stock_name)))

    for thread in user_threads:
        thread.start()

    for thread in user_threads:
        thread.join()

    # migration_thread = Thread(target = periodic_migration, args=(1000))

if __name__ == '__main__':
    main()