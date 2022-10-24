from util import load_mysql_args, load_clickhouse_args, print_progress, COLUMN_NAMES, STOCK_NAMES
from time import sleep
from datetime import datetime
import mysql.connector
from clickhouse_driver import Client


def migrate_to_clickhouse(num_rows_for_bulk_insert=10000, delay_between_bulk_inserts=0.1):
    mydb = mysql.connector.connect(**load_mysql_args())

    #get data from mysql
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM stock_info")
    stock_info = mycursor.fetchall()

    #dump it to clickhouse
    client = Client(**load_clickhouse_args())
    client.execute('USE transactions')

    for i in range(0, len(stock_info), num_rows_for_bulk_insert):
        num_rows = min(num_rows_for_bulk_insert, len(stock_info) - i)
        print_progress(f'migrating {len(stock_info)} rows to Clickouse', i, len(stock_info))
        rows = stock_info[i:i+num_rows]
        sql_variables = ', '.join(['(%s)' % (', '.join([f'%(a{row_index}_{i})s' for i in range(len(rows[row_index]))])) for row_index in range(num_rows)])
        sql = "INSERT INTO stock_info (stockname, %s) VALUES %s" % (', '.join(COLUMN_NAMES), sql_variables)
        converted_val = [[(f'a{row_index}_{i}', rows[row_index][i]) for i in range(len(rows[row_index]))] for row_index in range(num_rows)]
        merged_dict = dict([item for sublist in converted_val for item in sublist])
        client.execute(sql, merged_dict)
        sleep(delay_between_bulk_inserts)
    print_progress(f'migrating {len(stock_info)} rows to Clickouse', len(stock_info), len(stock_info))

if __name__ == "__main__":
    migrate_to_clickhouse()
