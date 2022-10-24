from util import STOCK_NAMES, load_mysql_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP
from time import sleep
from datetime import datetime
from threading import Thread
import mysql.connector
        
time_to_wait = 0.0001
max_num_rows_per_stock = 10

def write_data(conn, cursor, stock_name):
    with open (STOCK_TO_DATA_FILE_NAME_MAP[stock_name], "r") as df:
        i = 0
        for line in df:
            # need to skip first row in csv since it is header row
            if i == 0:
                i = 1
                continue
            if i > max_num_rows_per_stock:
                break

            # the csv has the datetime in the format
            # YYYY-MM-DD HH:MM:SS+5:30
            # but we need it in
            # 'YYYY-MM-DD HH:MM'
            datetime_end_idx = line.find(',')
            assert(datetime_end_idx != -1)
            plus_idx = line.find('+')
            assert(plus_idx != -1)
            assert(plus_idx < 100)
            sql = f"INSERT INTO stock_info VALUES ('{stock_name}',\'{line[:plus_idx]}\'{line[datetime_end_idx:]})"
            print(sql)
            cursor.execute(sql)
            conn.commit()
            sleep(time_to_wait)
            i += 1

if __name__ == "__main__":
    mysql_conn = mysql.connector.connect(**load_mysql_args())
    mysql_cursor = mysql_conn.cursor()
    write_data(mysql_conn, mysql_cursor, STOCK_NAMES[0])