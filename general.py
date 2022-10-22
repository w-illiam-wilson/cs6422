import json
import sys

def print_progress(desc, i, n):
    sys.stdout.write('\r')
    j = (i + 1) / n
    sys.stdout.write("Progress %s: [%-20s] %d%%" % (desc,'='*int(20*j), 100*j))
    sys.stdout.flush()

def load_mysql_args():
    username = ""
    password = ""
    no_password = False

    with open('config.json') as f:
        data = json.load(f)
        username = data['username']
        if "no_password" in data.keys():
            no_password = True
        else:
            password = data['password']

    args = {
        "host": "localhost",
        "port": "3306",
        "user": username,
        "database": "transactions"
    }

    if not no_password:
        args["password"] = password

    return args

def load_clickhouse_args():
    args = {
        "host": "localhost",
    }
    password = ""
    no_password = False
    with open('config.json') as f:
        data = json.load(f)
        if "no_password_clickhouse" in data.keys():
            no_password = True
        else:
            if "password_clickhouse" in data.keys():
                password = data['password_clickhouse']
            elif "password" in data.keys():
                password = data['password']
    if not no_password:
        args["password"] = password
    return args
