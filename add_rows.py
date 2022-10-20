import json
from time import sleep
from datetime import datetime
import mysql.connector

username = None
password = None
time_to_wait = 0.01
username = ""
password = ""

with open('config.json') as f:
    data = json.load(f)
    username = data['username']
    password = data['password']

print(username)
print(password)


mydb = mysql.connector.connect(
  host="localhost",
  user=username,
  password=password,
  database="transactions"
)

mycursor = mydb.cursor()

while True:
    sleep(time_to_wait)
    print("Hello")
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sql = "INSERT INTO purchases (time_purchase, customer_id, cost, store_num) VALUES (%s, %s, %s, %s)"
    val = (formatted_date, 1, 2, 3)
    mycursor.execute(sql, val)
    mydb.commit()
