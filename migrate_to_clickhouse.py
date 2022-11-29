from util import load_mysql_args, load_clickhouse_args, print_progress, COLUMN_NAMES, STOCK_NAMES
from time import sleep, perf_counter
from datetime import datetime
import mysql.connector
from clickhouse_driver import Client

def add_dirty_rows(mysql_conn, mysql_cursor, tuple_list, transaction_type):
    key_str = ', '.join([f'({stockname}, {date}, 0)' for stockname, date in tuple_list])
    existing_entries_sql = f"SELECT stockname,date from dirty_table WHERE (stockname, date, updating) IN {key_str}"
    mysql_cursor.execute(existing_entries_sql)
    existing_tuples = mysql_cursor.fetchall()
    to_insert = [tup for tup in tuple_list if tup not in existing_tuples]
    to_insert_str = ', '.join([f'({stockname}, {date}, 0, {transaction_type})' for stockname, date in to_insert])
    insert_sql = f"INSERT INTO dirty_table (stockname, date, updating, transaction_type) VALUES {to_insert_str}"
    for tup in existing_tuples:
        update_sql = f"UPDATE dirty_table SET transaction_type = {transaction_type} WHERE stockname={tup[0]} AND date={tup[1]} AND updating=0"
        mysql_cursor.execute(update_sql)
    mysql_cursor.execute(insert_sql)
    mysql_conn.commit()

# WORK IN PROGRESS
def migrate_to_clickhouse(mysql_conn, mysql_cursor, clickhouse_client, num_rows_for_bulk_insert=10000, delay_between_bulk_inserts=0.1):

    tic = perf_counter()
    for i in range(0, len(stock_info), num_rows_for_bulk_insert):
        num_rows = min(num_rows_for_bulk_insert, len(stock_info) - i)
        print_progress(f'migrating {len(stock_info)} rows to Clickouse', i, len(stock_info))
        rows = stock_info[i:i+num_rows]
        sql_variables = ', '.join(['(%s)' % (', '.join([f'%(a{row_index}_{i})s' for i in range(len(rows[row_index]))])) for row_index in range(num_rows)])
        sql = "INSERT INTO stock_info (stockname, %s) VALUES %s" % (', '.join(COLUMN_NAMES), sql_variables)
        converted_val = [[(f'a{row_index}_{i}', rows[row_index][i]) for i in range(len(rows[row_index]))] for row_index in range(num_rows)]
        merged_dict = dict([item for sublist in converted_val for item in sublist])
        clickhouse_client.execute(sql, merged_dict)
        sleep(delay_between_bulk_inserts)
    print_progress(f'migrating {len(stock_info)} rows to Clickouse', len(stock_info), len(stock_info))
    toc = perf_counter() 
    print(f"\nMigrated in {toc - tic:0.4f} seconds")
if __name__ == "__main__":
    migrate_to_clickhouse()
