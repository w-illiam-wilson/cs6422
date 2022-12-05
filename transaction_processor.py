from util import load_mysql_args, load_clickhouse_args
import time
import mysql.connector
from clickhouse_driver import Client
import sqlparse

def whichTo(selection):
    if selection in ["olap", "ola", "ap", "a"]:
        return "olap"
    elif selection in ["oltp", "olt", "tp", "t"]:
        return "oltp"
    else:
        return None

# Adapted from https://stackoverflow.com/questions/60822203/how-to-parse-any-sql-get-columns-names-and-table-name-using-sql-parser-in-python
def get_query_columns(stmt):
    columns = set()
    column_identifiers = []

    # get column_identifieres
    in_select = False
    for token in stmt.tokens:
        if isinstance(token, sqlparse.sql.Comment):
            continue
        if str(token).lower() == 'select':
            in_select = True
        elif in_select and token.ttype == sqlparse.tokens.Wildcard:
            columns.add("*")
        elif in_select and token.ttype is None:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    column_identifiers.append(identifier)
            elif isinstance(token, sqlparse.sql.Identifier):
                column_identifiers.append(token)
            break

    # get column names
    for column_identifier in column_identifiers:
        columns.add(column_identifier.get_name())

    return columns
    
def select_db_from_query(query):
    parsed_query = sqlparse.parse(query)
    operator = parsed_query[0].get_type()
    print(operator.lower())
    if operator.upper() == 'SELECT':
        query_columns = get_query_columns(parsed_query[0])
        if "*" in query_columns or len(query_columns) > 30:
            return "oltp"
        return "olap"
    elif operator.upper() in ['INSERT', 'UPDATE', 'DELETE']:
        return "oltp"
    else:
        print(f"Error selecting database for querying. Operator identified was {operator}")
        return None

time_to_wait = 0.01

mydb = mysql.connector.connect(**load_mysql_args())
mycursor = mydb.cursor()

client = Client(**load_clickhouse_args())
client.execute("USE transactions")

query = ""

while(True):
    query = input("Type any query: ")
    which = select_db_from_query(query)
    while which is None:
        query = input("Error parsing query, please retry: ")
        which = select_db_from_query(query)
    try:
        print(f"Targetting database for {which}")
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
        print("Failed to process query")


#select queries for mysql
#start timer

#query

#stop timer



#select queries for clickhouse
