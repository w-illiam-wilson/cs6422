from util import print_progress, COLUMN_NAMES, fmt, format_date
from time import sleep, perf_counter

def add_dirty_rows(mysql_conn, mysql_cursor, tuple_list, transaction_type):
    formatted_tuples = [(fmt(tup[0]), fmt(tup[1])) for tup in tuple_list]
    key_str = ', '.join([f'({stockname}, {date}, 0)' for stockname, date in formatted_tuples])
    existing_entries_sql = f"SELECT stockname,date from dirty_table WHERE (stockname, date, updating) IN ({key_str})"
    mysql_cursor.execute(existing_entries_sql)
    existing_tuples = mysql_cursor.fetchall()
    to_insert = [formatted_tuples[i] for i,tup in enumerate(tuple_list) if tup not in existing_tuples]
    to_insert_str = ', '.join([f'({stockname}, {date}, 0, {transaction_type})' for stockname, date in to_insert])
    existing_tuples_str = ', '.join([f'({stockname}, {date}, 0, {transaction_type})' for stockname, date in existing_tuples])
    if len(to_insert) > 0:
        insert_sql = f"INSERT INTO dirty_table (stockname, date, updating, transaction_type) VALUES {to_insert_str}"
        mysql_cursor.execute(insert_sql)
    if len(existing_tuples):
        update_sql = f"INSERT INTO dirty_table (stockname, date, updating, transaction_type) VALUES {existing_tuples_str} \
        ON DUPLICATE KEY UPDATE stockname=VALUES(stockname), date=VALUES(date), transaction_type={transaction_type}, updating=0"
        mysql_cursor.execute(update_sql)
    mysql_conn.commit()

def get_dirty_rows(mysql_cursor, transaction_type):
    get_dirty_tuples_sql = f"SELECT stockname,date from dirty_table WHERE updating=1 AND transaction_type={transaction_type}"
    mysql_cursor.execute(get_dirty_tuples_sql)
    return mysql_cursor.fetchall()

def migrate_to_clickhouse(mysql_conn, mysql_cursor, clickhouse_client):
    tic = perf_counter()
    update_sql = f"UPDATE dirty_table SET updating=1"
    mysql_cursor.execute(update_sql)

    dirty_upsert_tuples = get_dirty_rows(mysql_cursor, 0)
    fetch_rows_sql = f"SELECT * from stock_info where stockname IN ({','.join([fmt(t[0]) for t in dirty_upsert_tuples])}) AND date IN ({','.join([fmt(t[1]) for t in dirty_upsert_tuples])})"
    mysql_cursor.execute(fetch_rows_sql)
    upsert_rows = mysql_cursor.fetchall()
    if len(upsert_rows) > 0:
        print_progress(f'migrating {len(upsert_rows)} upsert rows to Clickhouse', 0, len(upsert_rows))
        upsert_rows_string = ', '.join([str((upsert_rows[row_index][0], str(upsert_rows[row_index][1])) + upsert_rows[row_index][2:]) for row_index in range(len(upsert_rows))])
        upsert_sql = "INSERT INTO stock_info (stockname, %s) VALUES %s" % (', '.join(COLUMN_NAMES), upsert_rows_string)
        clickhouse_client.execute(upsert_sql)
        print_progress(f'migrating {len(upsert_rows)} upsert rows to Clickhouse', len(upsert_rows), len(upsert_rows))

    dirty_delete_tuples = get_dirty_rows(mysql_cursor, 1)
    if len(dirty_delete_tuples) > 0:
        print_progress(f'migrating {len(dirty_delete_tuples)} delete rows to Clickhouse', 0, len(dirty_delete_tuples))
        delete_sql = f"DELETE FROM stock_info WHERE stockname in {','.join([fmt(t[0]) for t in dirty_delete_tuples])} AND date IN {','.join([fmt(t[1]) for t in dirty_delete_tuples])}"
        clickhouse_client.execute(delete_sql)
        print_progress(f'migrating {len(dirty_delete_tuples)} delete rows to Clickhouse', len(dirty_delete_tuples), len(dirty_delete_tuples))

    mysql_cursor.execute("DELETE FROM dirty_table WHERE updating=1")
    mysql_conn.commit()

    toc = perf_counter() 
    print(f"\nMigrated in {toc - tic:0.4f} seconds")
if __name__ == "__main__":
    migrate_to_clickhouse()
