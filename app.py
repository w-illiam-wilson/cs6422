from util import load_mysql_args, load_clickhouse_args, print_progress, STOCK_TO_DATA_FILE_NAME_MAP
from add_rows import write_data
from threading import Thread
import time
from clickhouse_driver import Client
from workloads import hybrid_insert_aggregate_workload
import mysql.connector

class App():
    # creates a snapshot every [period] seconds
    # or every [snapshot_query_threshold] queries,
    # if [periodic_migration] or [snapshot_query] is true, respectively.
    def __init__(self, periodic_migration = True, period = 10000, snapshot_query = True, snapshot_query_threshold = 1000):
        # print("Connecting to MySQL database...")
        self.mysql_conn = mysql.connector.connect(**load_mysql_args())
        self.mysql_cursor = self.mysql_conn.cursor(buffered=True)

        # print("Init MySQL database...")
        fd = open('create_database.sql', 'r')
        create_sql_db_script = fd.read()
        fd.close()
        for command in create_sql_db_script.split(';\n'):
            # print(command)
            self.mysql_cursor.execute(command)
            self.mysql_conn.commit()

        # print("Connecting to Clickhouse database...")
        self.clickhouse_client = Client(**load_clickhouse_args())

        # print("Init Clickhouse database...")
        fd = open('create_database_clickhouse.sql', 'r')
        create_clickhouse_db_script = fd.read()
        fd.close()
        for command in create_clickhouse_db_script.split(';\n'):
            # print(command)
            self.clickhouse_client.execute(command)

        self.num_queries_executed = 0
        self.periodic_migration_should_start = periodic_migration
        self.period = period
        self.snapshot_query = snapshot_query
        self.snapshot_query_threshold = snapshot_query_threshold

        # timing
        self.total_time = 0
        self.migration_time = 0

    def cleanup(self):
        self.mysql_cursor.close()
        self.mysql_conn.close()


    def migrate_to_clickhouse(self, num_rows_for_bulk_insert=100000):
        #get data from mysql
        tic = time.perf_counter()
        self.mysql_cursor.execute("SELECT * FROM stock_info")
        stock_info = self.mysql_cursor.fetchall()

        for i in range(0, len(stock_info), num_rows_for_bulk_insert):
            num_rows = min(num_rows_for_bulk_insert, len(stock_info) - i)
            print_progress(f'migrating {len(stock_info)} rows to Clickhouse', i, len(stock_info))
            rows = stock_info[i:i+num_rows]
            sql_variables = ', '.join(['(%s)' % (', '.join([f'%(a{row_index}_{i})s' for i in range(len(rows[row_index]))])) for row_index in range(num_rows)])
            sql = "INSERT INTO stock_info VALUES %s" % (sql_variables)
            converted_val = [[(f'a{row_index}_{i}', rows[row_index][i]) for i in range(len(rows[row_index]))] for row_index in range(num_rows)]
            merged_dict = dict([item for sublist in converted_val for item in sublist])
            self.clickhouse_client.execute(sql, merged_dict)

        toc = time.perf_counter() 
        print(f"\nMigrated rows in {toc - tic:0.4f} seconds\n")
        self.migration_time += toc - tic

    def start_periodic_migration(self):
        if not self.periodic_migration_should_start:
            return

        def periodic_migration_func():
            while True:
                time.sleep(self.period)
                migrate_to_clickhouse()
        migration_thread = Thread(target = periodic_migration, args=(10,))
        migration_thread.start()
        self.periodic_migration_should_start = False

    def write_mysql(self, query):
        tic = time.perf_counter()
        self.mysql_cursor.execute(query)
        self.mysql_conn.commit()
        self.start_periodic_migration()
        self.num_queries_executed += 1
        if self.snapshot_query and self.num_queries_executed > self.snapshot_query_threshold:
            self.num_queries_executed = 0
            print("Migrating to Clickhouse...")
            self.migrate_to_clickhouse()
        toc = time.perf_counter()
        self.total_time += toc-tic

    def write_clickhouse(self, query):
        tic = time.perf_counter()
        self.clickhouse_client.execute(query)
        self.start_periodic_migration() 
        toc = time.perf_counter()
        self.total_time += toc-tic



def main():
    max_rows_in_workload = 500000

    print("\nmixed")
    app = App(periodic_migration = False, snapshot_query = True, snapshot_query_threshold = 20)
    hybrid_insert_aggregate_workload(app, max_rows_in_workload=max_rows_in_workload)
    print("mixed insert")
    print(app.total_time)
    __start = app.total_time
    hybrid_update_aggregate_workload(app, max_rows_in_workload=max_rows_in_workload)
    print("mixed update")
    print(app.total_time - __start)
    __start = app.total_time
    hybrid_delete_aggregate_workload(app, max_rows_in_workload=max_rows_in_workload)
    print("mixed delete")
    print(app.total_time - __start)
    print("total time")
    print(app.total_time)
    print(app.migration_time)
    app.cleanup()

    print("\nmysql only")
    app = App(periodic_migration = False, snapshot_query = False, snapshot_query_threshold = 20)
    hybrid_insert_aggregate_workload(app, use_clickhouse_for_olap = False, max_rows_in_workload=max_rows_in_workload)
    print("mysql only insert")
    print(app.total_time)
    __start = app.total_time
    hybrid_update_aggregate_workload(app, use_clickhouse_for_olap = False, max_rows_in_workload=max_rows_in_workload)
    print("mysql only update")
    print(app.total_time - __start)
    __start = app.total_time
    hybrid_delete_aggregate_workload(app, use_clickhouse_for_olap = False, max_rows_in_workload=max_rows_in_workload)
    print("mysql only delete")
    print(app.total_time - __start)
    print("total time")
    print(app.total_time)
    print(app.migration_time)
    app.cleanup()

    print("\nclickhouse only")
    app = App(periodic_migration = False, snapshot_query = False, snapshot_query_threshold = 20)
    hybrid_insert_aggregate_workload(app, use_mysql_for_oltp = False, max_rows_in_workload=max_rows_in_workload)
    print("clickhouse only insert")
    print(app.total_time)
    __start = app.total_time
    hybrid_update_aggregate_workload(app, use_mysql_for_oltp = False, max_rows_in_workload=max_rows_in_workload)
    print("clickhouse only update")
    print(app.total_time - __start)
    __start = app.total_time
    hybrid_delete_aggregate_workload(app, use_mysql_for_oltp = False, max_rows_in_workload=max_rows_in_workload)
    print("clickhouse only delete")
    print(app.total_time - __start)
    print("total time")
    print(app.total_time)
    print(app.migration_time)
    app.cleanup()

    # TODO write a client class which contains write_data() and migrate_to_clickhouse()
    # so that migrate_to_clickhouse() can block the write_data() when it is running
    # may or may not be necessary depending on if mysql.connector is thread safe.

    # user_threads = []
    # for stock_name in STOCK_TO_DATA_FILE_NAME_MAP.keys():
    #     conn = mysql.connector.connect(**load_mysql_args())
    #     cursor = conn.cursor()
    #     user_threads.append(Thread(target = write_data, args=(conn, cursor, stock_name)))

    # for thread in user_threads:
    #     thread.start()
    # migration_thread = Thread(target = periodic_migration, args=(10,))
    # migration_thread.start()

    # for thread in user_threads:
    #     thread.join()
    # migration_thread.join()

if __name__ == '__main__':
    main()
