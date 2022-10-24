from general import load_mysql_args, load_clickhouse_args, print_progress, COLUMN_NAMES
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

        sql = "INSERT INTO stock_info (stockname, "
        val = row
        for i in range(len(COLUMN_NAMES) - 1):
            sql += COLUMN_NAMES[i] + ","
        sql += COLUMN_NAMES[len(COLUMN_NAMES) - 1]
        sql+=") VALUES ("
        for i in range(len(val) - 1):
            sql += "%(a" + str(i + 1) + ")s, "
        sql += "%(a" + str(len(val)) + ")s)"
        converted_val = {('a' + str(i + 1)): val[i] for i in range(len(val))}
        client.execute(sql, converted_val)

if __name__ == "__main__":
    migrate_to_clickhouse()


