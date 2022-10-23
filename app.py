from general import load_mysql_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP
from add_rows import write_data
from threading import Thread
import mysql.connector

def periodic_migration(period):
    while True:
        thread.sleep(period)
        migrate_to_clickhouse()

def main():
    print("Connecting to MySQL database...")
    mysql_conn = mysql.connector.connect(**load_mysql_args())
    mysql_cursor = mysql_conn.cursor()

    print("Init MySQL database...")
    fd = open('create_database.sql', 'r')
    create_sql_db_script = fd.read()
    fd.close()
    for command in create_sql_db_script.split(';'):
        # print(command)
        mysql_cursor.execute(command)
        mysql_conn.commit()

    print("Init Clickhouse database...")
    # TODO

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