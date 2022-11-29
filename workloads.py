from util import STOCK_NAMES, load_mysql_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP, COLUMN_NAMES
import time

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
                    to_insert = []

                    oltp_time_start = time.perf_counter()
                    if use_mysql_for_oltp:
                        app.write_mysql(oltp_query)
                    else:
                        app.write_clickhouse(oltp_query)
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
                                app.write_mysql(olap_query)
                            olap_time_end = time.perf_counter()
                            # print(f"olap time {olap_time_end - olap_time_start:0.4f}")
                            total_time += olap_time_end - olap_time_start

    print(f"{total_time:0.4f}")


# UPDATE table_name
# SET column1 = value1, column2 = value2, ...
# WHERE condition;
def hybrid_update_aggregate_workload(
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

    oltp_query = f"UPDATE stock_info SET open = 1000 WHERE stockname = {STOCK_NAMES[0]};"
    to_insert = []

    oltp_time_start = time.perf_counter()
    if use_mysql_for_oltp:
        app.write_mysql(oltp_query)
    else:
        app.write_clickhouse(oltp_query)
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
                app.write_mysql(olap_query)
            olap_time_end = time.perf_counter()
            # print(f"olap time {olap_time_end - olap_time_start:0.4f}")
            total_time += olap_time_end - olap_time_start

    print(f"{total_time:0.4f}")