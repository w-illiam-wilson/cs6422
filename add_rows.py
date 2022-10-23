from general import load_mysql_args, print_progress, DATA_FILE_NAMES, COLUMN_NAMES
from time import sleep
from datetime import datetime
from threading import Thread
import mysql.connector
        
time_to_wait = 0.0001

def write_data(conn, cursor, data_file):
    with open (data_file, "r") as df:
        i = 0
        for line in df:
            if i == 0:
                i = 1
                continue
            if i > 10:
                break
            print(data_file)

            # the csv has the datetime in the format
            # YYYY-MM-DD HH:MM:SS+5:30
            # but we need it in
            # 'YYYY-MM-DD HH:MM'
            datetime_end_idx = line.find(',')
            assert(datetime_end_idx != -1)
            plus_idx = line.find('+')
            assert(plus_idx != -1)
            assert(plus_idx < 100)
            sql = f"INSERT INTO stock_info VALUES (\'{line[:plus_idx]}\'{line[datetime_end_idx:]})"
            print(sql)
            cursor.execute(sql)
            conn.commit()
            sleep(time_to_wait)
            i += 1

threads = []
for data_file_name in DATA_FILE_NAMES:
    conn = mysql.connector.connect(**load_mysql_args())
    cursor = conn.cursor()
    threads.append(Thread(target = write_data, args=(conn, cursor, data_file_name)))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
