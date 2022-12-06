from util import STOCK_NAMES, date_string_to_obj, get_column_slice, get_random_values, load_mysql_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP, COLUMN_NAMES, fmt, format_date
import time
import random

"""
inserts [oltp_per_olap_burst] rows from a stock sequentially,
using mysql if [use_mysql_for_oltp] and clickhouse otherwise.
Then performs a olap burst consisting of aggregate commands,
(max, min, sum) over all columns,
using clickhouse if [use_clickhouse_for_olap] and mysql otherwise.
"""
OLAP_QUERIES = [f"SELECT MAX({col_name}) FROM stock_info GROUP BY stockname" for col_name in COLUMN_NAMES if col_name not in ['stockname', 'date']] \
    + [f"SELECT MIN({col_name}) FROM stock_info GROUP BY stockname" for col_name in COLUMN_NAMES if col_name not in ['stockname', 'date']] \
    + [f"SELECT SUM({col_name}) FROM stock_info GROUP BY stockname" for col_name in COLUMN_NAMES if col_name not in ['stockname', 'date']] \
    # + [f"SELECT AVG({col_name}) FROM stock_info GROUP BY stockname" for col_name in COLUMN_NAMES if col_name not in ['stockname', 'date']]

def hybrid_insert_aggregate_workload(
        app,
        oltp_per_olap_burst = 20000,
        use_mysql_for_oltp = True,
        use_clickhouse_for_olap = True,
        rows_per_insert = 2000,
        max_rows_in_workload = float('inf')
    ):

    i = 0
    total_rows = 0
    total_time = 0

    for stock_name in STOCK_NAMES:
        with open (STOCK_TO_DATA_FILE_NAME_MAP[stock_name], "r") as df:
            is_first_line = True
            to_insert = []
            for line in df:
                if is_first_line:
                    is_first_line = False
                    continue
                
                if total_rows > max_rows_in_workload:
                    print(f"{total_time:0.4f}")
                    return

                # the csv has the datetime in the format
                # YYYY-MM-DD HH:MM:SS+5:30
                # but we need it in
                # 'YYYY-MM-DD HH:MM'
                datetime_end_idx = line.find(',')
                # assert(datetime_end_idx != -1)
                plus_idx = line.find('+')
                # assert(plus_idx != -1)
                # assert(plus_idx < 100)
                to_insert.append(f"'{stock_name}',\'{line[:plus_idx]}\'{line[datetime_end_idx:]}")
                if len(to_insert) >= rows_per_insert:
                    tuples = ', '.join([f'({tup})' for tup in to_insert])
                    oltp_query = f"INSERT INTO stock_info VALUES {tuples}"

                    oltp_time_start = time.perf_counter()
                    if use_mysql_for_oltp:
                        split_tuples = [tup.split(',') for tup in to_insert]
                        new_tuple_keys = [(tup[0][1:-1],date_string_to_obj(tup[1][1:-1])) for tup in split_tuples]
                        app.write_mysql(oltp_query,'INSERT',new_tuple_keys)
                    else:
                        app.write_clickhouse(oltp_query)
                    to_insert = []
                    oltp_time_end = time.perf_counter()
                    total_time += oltp_time_end - oltp_time_start

                    i += rows_per_insert
                    total_rows += rows_per_insert
                    if i > oltp_per_olap_burst:
                        i = 0
                        # run olap burst
                        for olap_query in OLAP_QUERIES:
                            olap_time_start = time.perf_counter()
                            if use_clickhouse_for_olap:
                                app.write_clickhouse(olap_query)
                            else:
                                app.write_mysql(olap_query,'SELECT')
                            olap_time_end = time.perf_counter()
                            # print(f"olap time {olap_time_end - olap_time_start:0.4f}")
                            total_time += olap_time_end - olap_time_start

    print(f"{total_time:0.4f}")

def hybrid_update_aggregate_workload(
        app,
        oltp_per_olap_burst = 20000,
        use_mysql_for_oltp = True,
        use_clickhouse_for_olap = True,
        rows_per_insert = 5000,
        max_rows_in_workload = float('inf')
    ):
    # variables for selecting proper rows to update
    query_function = app.get_mysql_query_results if use_mysql_for_oltp else app.get_clickhouse_query_results
    rows_in_table = query_function('select count(*) from stock_info')[0][0]
    num_rows_to_update = min([rows_in_table, rows_per_insert])
    update_index = 0
    columns_to_update = 4
    column_start_index = 1 # actual index is 1 + column_index, total number of columns is len(COLUMN_NAMES)-1

    # loop variables
    i = 0
    total_rows = 0
    total_time = 0
    num_olap_executions = 0
    while num_olap_executions < 5:
        tuples_to_update = query_function(f'select stockname,date from stock_info LIMIT {update_index},{num_rows_to_update}')
        column_names_to_update = get_column_slice(column_start_index, columns_to_update)
        column_str = ','.join(column_names_to_update)
        random_values = [get_random_values(update_index, num_rows_to_update) for _ in range(columns_to_update)]
        update_values = ', '.join([str((tup[0],format_date(tup[1])) + tuple([random_values[i][j] for i in range(columns_to_update)])) for j,tup in enumerate(tuples_to_update)])
        update_string = ', '.join([f'{col_name} = VALUES({col_name})' for col_name in ['stockname','date'] + column_names_to_update])
        oltp_query_mysql = f'INSERT INTO stock_info (stockname,date,{column_str}) VALUES {update_values} ON DUPLICATE KEY UPDATE {update_string}'
        oltp_query_clickhouse = f"INSERT INTO stock_info (stockname,date,{column_str}) VALUES {update_values}"
        update_index = (update_index + num_rows_to_update) % rows_in_table
        column_start_index += columns_to_update
        if column_start_index > len(COLUMN_NAMES) - columns_to_update:
            column_start_index = 1
        oltp_time_start = time.perf_counter()
        if use_mysql_for_oltp:
            app.write_mysql(oltp_query_mysql,'UPDATE',tuples_to_update)
        else:
            app.write_clickhouse(oltp_query_clickhouse)
        oltp_time_end = time.perf_counter()
        total_time += oltp_time_end - oltp_time_start
        i += num_rows_to_update
        total_rows += num_rows_to_update
        if i > oltp_per_olap_burst:
            num_olap_executions += 1
            i = 0
            # run olap burst
            for olap_query in OLAP_QUERIES:
                olap_time_start = time.perf_counter()
                if use_clickhouse_for_olap:
                    app.write_clickhouse(olap_query)
                else:
                    app.write_mysql(olap_query,'SELECT')
                olap_time_end = time.perf_counter()
                # print(f"olap time {olap_time_end - olap_time_start:0.4f}")
                total_time += olap_time_end - olap_time_start

    print(f"{total_time:0.4f}")

def hybrid_delete_aggregate_workload(
        app,
        oltp_per_olap_burst = 20000,
        use_mysql_for_oltp = True,
        use_clickhouse_for_olap = True,
        rows_per_delete = 100,
        max_rows_in_workload = float('inf')
    ):

    i = 0
    total_rows = 0
    total_time = 0
    num_olap_executions = 0

    while num_olap_executions < 5:

        get_stock_count_mysql = f"SELECT stockname, count(*) FROM stock_info GROUP BY stockname;"
        get_stock_count_clickhouse = f"SELECT stockname, count(*) FROM stock_info GROUP BY stockname;"

        oltp_time_start = time.perf_counter()
        if use_mysql_for_oltp:
            stock_count = app.get_mysql_query_results(get_stock_count_mysql)
            if len(stock_count) > 0:
                stock_idx = random.randint(0, len(stock_count)-1)
                # delete_index = random.randint(0, stock_count[0][1] - rows_per_delete)
                # oltp_query_mysql = f"DELETE FROM stock_info WHERE stockname='{stock_count[0][0]}' LIMIT {rows_per_delete} OFFSET {delete_index};"
                oltp_query_mysql = f"DELETE FROM stock_info WHERE stockname='{stock_count[stock_idx][0]}' LIMIT {rows_per_delete};"
                app.write_mysql(oltp_query_mysql,'DELETE')
        else:
            stock_count = app.get_clickhouse_query_results(get_stock_count_mysql)
            if len(stock_count) > 0:
                stock_idx = random.randint(0, len(stock_count)-1)
                # delete_index = random.randint(0, stock_count[0][1] - rows_per_delete)
                # oltp_query_clickhouse = f"DELETE FROM stock_info WHERE stockname='{stock_count[0][0]}' LIMIT {rows_per_delete} OFFSET {delete_index};"
                oltp_query_clickhouse = f"ALTER TABLE stock_info DELETE WHERE (stockname, date) in (SELECT stockname, date from stock_info where stockname='{stock_count[stock_idx][0]}' LIMIT {rows_per_delete});"
                # print(oltp_query_clickhouse)
                app.write_clickhouse(oltp_query_clickhouse)
        oltp_time_end = time.perf_counter()
        total_time += oltp_time_end - oltp_time_start

        i += rows_per_delete
        total_rows += rows_per_delete
        if i > oltp_per_olap_burst:
            i = 0
            # run olap burst
            for olap_query in OLAP_QUERIES:
                olap_time_start = time.perf_counter()
                if use_clickhouse_for_olap:
                    app.write_clickhouse(olap_query)
                else:
                    app.write_mysql(olap_query,'DELETE')
                olap_time_end = time.perf_counter()
                # print(f"olap time {olap_time_end - olap_time_start:0.4f}")
                total_time += olap_time_end - olap_time_start
            num_olap_executions += 1

    print(f"{total_time:0.4f}")