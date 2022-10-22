from general import load_mysql_args, load_clickhouse_args, print_progress
from time import sleep
from datetime import datetime
import mysql.connector
from clickhouse_driver import Client

mydb = mysql.connector.connect(**load_mysql_args())

#get data from mysql
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM purchases")
purchases = mycursor.fetchall()

#dump it to clickhouse
client = Client(**load_clickhouse_args())

results = client.execute('SHOW DATABASES')
print(results)
time_to_wait = 0.01
i = 0
client.execute('USE transactions')
for row in purchases:
    # sleep(time_to_wait)
    i += 1
    print_progress('migrating to Clickouse', i, len(purchases))
    now = datetime.now()
    formatted_date = row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sql = "INSERT INTO purchases (time_purchase, customer_id, cost, store_num) VALUES (%(a1)s, %(a2)s, %(a3)s, %(a4)s)"
    val = (formatted_date, ) + row[1:]
    converted_val = {}
    for i in range(len(val)):
        converted_val['a' + str(i + 1)] = val[i]
    client.execute(sql, converted_val)


