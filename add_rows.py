from general import load_mysql_args, print_progress
from time import sleep
from datetime import datetime
import mysql.connector

mydb = mysql.connector.connect(**load_mysql_args())

mycursor = mydb.cursor()

time_to_wait = 0.0001
num_transactions = 100

for i in range(num_transactions):
    sleep(time_to_wait)
    print_progress("writing to MySQL", i, num_transactions)
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sql = "INSERT INTO purchases (time_purchase, customer_id, cost, store_num) VALUES (%s, %s, %s, %s)"
    val = (formatted_date, 1, 2, 3)
    mycursor.execute(sql, val)
    mydb.commit()
