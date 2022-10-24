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

        sql = "INSERT INTO stock_info (stockname, date, open, high, low, close, volume, sma5, sma10, sma15, sma20, ema5, ema10, ema15, ema20, upperband, middleband, lowerband, HT_TRENDLINE, KAMA10, KAMA20, KAMA30, SAR, TRIMA5, TRIMA10, TRIMA20, ADX5, ADX10, ADX20, APO, CCI5, CCI10, CCI15, macd510, macd520, macd1020, macd1520, macd1226, MOM10, MOM15, MOM20, ROC5, ROC10, ROC20, PPO, RSI14, RSI8, slowk, slowd, fastk, fastd, fastksr, fastdsr, ULTOSC, WILLR, ATR, Trange, TYPPRICE, HT_DCPERIOD, BETA) VALUES (%(a1)s, %(a2)s, %(a3)s, %(a4)s, %(a5)s, %(a6)s, %(a7)s, %(a8)s, %(a9)s, %(a10)s, %(a11)s, %(a12)s, %(a13)s, %(a14)s, %(a15)s, %(a16)s, %(a17)s, %(a18)s, %(a19)s, %(a20)s, %(a21)s, %(a22)s, %(a23)s, %(a24)s, %(a25)s, %(a26)s, %(a27)s, %(a28)s, %(a29)s, %(a30)s, %(a31)s, %(a32)s, %(a33)s, %(a34)s, %(a35)s, %(a36)s, %(a37)s, %(a38)s, %(a39)s, %(a40)s, %(a41)s, %(a42)s, %(a43)s, %(a44)s, %(a45)s, %(a46)s, %(a47)s, %(a48)s, %(a49)s, %(a50)s, %(a51)s, %(a52)s, %(a53)s, %(a54)s, %(a55)s, %(a56)s, %(a57)s, %(a58)s, %(a59)s, %(a60)s)"
        val = row
        converted_val = {('a' + str(i + 1)): val[i] for i in range(len(val))}
        print(converted_val)
        client.execute(sql, converted_val)

if __name__ == "__main__":
    migrate_to_clickhouse()


