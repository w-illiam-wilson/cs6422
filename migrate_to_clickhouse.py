from util import print_progress, COLUMN_NAMES
from time import sleep, perf_counter

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

def get_dirty_rows(mysql_cursor, transaction_type):
    get_dirty_tuples_sql = f"SELECT stockname,date from dirty_table WHERE updating=1 AND transaction_type={transaction_type}"
    mysql_cursor.execute(get_dirty_tuples_sql)
    return mysql_cursor.fetchall()

# WORK IN PROGRESS
def migrate_to_clickhouse(mysql_conn, mysql_cursor, clickhouse_client, num_rows_for_bulk_insert=10000, delay_between_bulk_inserts=0.1):
    tic = perf_counter()
    update_sql = f"UPDATE dirty_table SET updating=1"
    mysql_cursor.execute(update_sql)

    dirty_upsert_tuples = get_dirty_rows(mysql_cursor, 0)
    fetch_rows_sql = f"SELECT * from stock_info where stockname IN ({','.join([str(t[0]) for t in dirty_upsert_tuples])}) AND date IN ({','.join([str(t[1]) for t in dirty_upsert_tuples])})"
    mysql_cursor.execute(fetch_rows_sql)
    upsert_rows = mysql_cursor.fetchall()
    upsert_rows_string = ', '.join(['(%s)' % (', '.join([f'%(a{row_index}_{i})s' for i in range(len(upsert_rows[row_index]))])) for row_index in range(len(upsert_rows))])
    upsert_sql = "INSERT INTO stock_info (stockname, %s) VALUES %s" % (', '.join(COLUMN_NAMES), upsert_rows_string)
    clickhouse_client.execute(upsert_sql)

    dirty_delete_tuples = get_dirty_rows(mysql_cursor, 1)
    delete_sql = f"DELETE FROM stock_info WHERE stockname in {','.join([str(t[0]) for t in dirty_delete_tuples])} AND date IN {','.join([str(t[1]) for t in dirty_delete_tuples])}"
    clickhouse_client.execute(delete_sql)

    mysql_cursor.execute("DELETE FROM dirty_table WHERE updating=1")
    mysql_conn.commit()

    toc = perf_counter() 
    print(f"\nMigrated in {toc - tic:0.4f} seconds")
if __name__ == "__main__":
    migrate_to_clickhouse()
