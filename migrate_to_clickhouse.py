from util import load_mysql_args, load_clickhouse_args, print_progress, COLUMN_NAMES, STOCK_NAMES
from time import sleep
from datetime import datetime
import mysql.connector
from clickhouse_driver import Client


def migrate_to_clickhouse():
    mydb = mysql.connector.connect(**load_mysql_args())

    #get data from mysql
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM stock_info")
    stock_info = mycursor.fetchall()

    #dump it to clickhouse
    client = Client(**load_clickhouse_args())

    time_to_wait = 0.01
    rowcount = 0
    client.execute('USE transactions')
    for row in stock_info:
        # sleep(time_to_wait)
        rowcount += 1
        print_progress('migrating to Clickouse', rowcount, len(stock_info))
        sql = "INSERT INTO stock_info (stockname, %s) VALUES (%s)" % (', '.join(COLUMN_NAMES), ', '.join([f'%(a{i+1})s' for i in range(len(row))]))
        converted_val = {f'a{i+1}': row[i] for i in range(len(row))}
        client.execute(sql, converted_val)

if __name__ == "__main__":
    migrate_to_clickhouse()
