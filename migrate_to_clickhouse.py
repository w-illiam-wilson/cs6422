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


#get data from mysql




#dump it to clickhouse
