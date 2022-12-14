from util import STOCK_NAMES, load_mysql_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP
from time import sleep
from datetime import datetime
from threading import Thread
import mysql.connector
#import pandas as pd

time_to_wait = 0.0001
max_num_rows_per_stock = 10000
rows_per_insert = 200

def do_insert(conn, cursor, to_insert):
    tuples = ', '.join([f'({tup})' for tup in to_insert])
    sql = f"INSERT INTO stock_info VALUES {tuples}"
    # print(sql)
    cursor.execute(sql)
    conn.commit()

def write_data(conn, cursor, stock_name):
    # results = pd.read_csv(STOCK_TO_DATA_FILE_NAME_MAP[stock_name])
    # length = len(results)
    with open (STOCK_TO_DATA_FILE_NAME_MAP[stock_name], "r") as df:
        i = 0
        to_insert = []
        for line in df:
            # print_progress(f"adding {max_num_rows_per_stock} rows of {stock_name} stock data to MySQL", i, max_num_rows_per_stock)
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
            to_insert.append(f"'{stock_name}',\'{line[:plus_idx]}\'{line[datetime_end_idx:]}")
            if len(to_insert) == rows_per_insert:
                do_insert(conn, cursor, to_insert)
                to_insert = []
                sleep(time_to_wait)
            i += 1
        if len(to_insert) > 0:
            do_insert(conn, cursor, to_insert)


if __name__ == "__main__":
    mysql_conn = mysql.connector.connect(**load_mysql_args())
    mysql_cursor = mysql_conn.cursor()
    write_data(mysql_conn, mysql_cursor, STOCK_NAMES[0])
