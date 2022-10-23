# CS 6422 Project
## General Installation
1. Install MySQL
2. Install Clickhouse: https://clickhouse.com/clickhouse#getting_started
3. Add your config using template and renaming file "config.json"
4. Make sure you have Python 3 (Code is specifically known to support 3.9), and you run `pip install -r requirements.txt` in the project directory
5. Download the [dataset](https://www.kaggle.com/datasets/debashis74017/stock-market-data-nifty-100-stocks-5-min-data) and extract its contents to `data/`

## DB Setup For WSL
Here's some general setup instructions for DB Setup on WSL:
1. Install MySQL Server. Here is one tutorial for how to do so: https://pen-y-fan.github.io/2021/08/08/How-to-install-MySQL-on-WSL-2-Ubuntu/
2. Make sure you run `sudo service mysqld start`. Then, navigate to this project's directory. Then, if you are not able to run `mysql` normally, run `sudo mysql -h 127.0.0.1 -P 3306 -u root` and enter your root password.
3. Once inside the MySQL shell, run `source create_database.sql`
4. Install Clickhouse. These steps worked for me: https://clickhouse.com/docs/en/install/#install-from-deb-packages. If you set a password, make sure to assign `password_clickhouse` in the config file.
5. Run `clickhouse-client --password <your_password_here> --queries-file create_database_clickhouse.sql`

## Script Descriptions
0. TODO: put all this functionality into app.py
1. run `mysql -u [username] -p < create_database.sql` to create the MySQL databse
2. Use `add_rows` to simulate users adding data (OLTP) to the MySQL database
3. Use `migrate_to_clickhouse` to migrate data from MySQL to Clickhouse
4. TODO describe others once complete
5. TODO benchmarking and OLTP/OLAP queries