from general import load_mysql_args, load_clickhouse_args, print_progress
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
    i = 0
    client.execute('USE transactions')
    for row in stock_info:
        # sleep(time_to_wait)
        i += 1
        print_progress('migrating to Clickouse', i, len(stock_info))

        sql = "INSERT INTO stock_info (stockname, date, open, high, low, close, volume, sma5, sma10, sma15, sma20, ema5, ema10, ema15, ema20, upperband, middleband, lowerband, HT_TRENDLINE, KAMA10, KAMA20, KAMA30, SAR, TRIMA5, TRIMA10, TRIMA20, ADX5, ADX10, ADX20, APO, CCI5, CCI10, CCI15, macd510, macd520, macd1020, macd1520, macd1226, MOM10, MOM15, MOM20, ROC5, ROC10, ROC20, PPO, RSI14, RSI8, slowk, slowd, fastk, fastd, fastksr, fastdsr, ULTOSC, WILLR, ATR, Trange, TYPPRICE, HT_DCPERIOD, BETA) VALUES ("
        val = row
        for i in range(len(val) - 1):
            sql += "%(a" + str(i + 1) + ")s, "
        sql += "%(a" + str(len(val)) + ")s)"
        converted_val = {('a' + str(i + 1)): val[i] for i in range(len(val))}
        client.execute(sql, converted_val)

if __name__ == "__main__":
    migrate_to_clickhouse()


