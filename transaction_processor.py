from util import load_mysql_args, load_clickhouse_args
import time
import mysql.connector
from clickhouse_driver import Client

def whichTo(selection):
    if selection in ["olap", "ola", "ap", "a"]:
        return "olap"
    elif selection in ["oltp", "olt", "tp", "t"]:
        return "oltp"
    else:
        return None

time_to_wait = 0.01

mydb = mysql.connector.connect(**load_mysql_args())
mycursor = mydb.cursor()

client = Client(**load_clickhouse_args())
client.execute("USE transactions")

query = ""

while(True):
    query = input("Type any query: ")
    which = None
    while which is None:
        chosen = input("olap (a) or oltp (t): ")
        which = whichTo(chosen)
    try:
        tic = time.perf_counter()
        if which == "oltp":
            mycursor.execute(query)
            stock_info = mycursor.fetchall()
        else:
            stock_info = client.execute(query)
        toc = time.perf_counter()
        print(stock_info)
        print(f"Executed in {toc - tic:0.4f} seconds")
    except:
        print("failed to process query")


#select queries for mysql
#start timer

#query

#stop timer



#select queries for clickhouse
