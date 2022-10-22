# CS 6422 Project
## General Installation
1. Install MySQL
2. Install Clickhouse: https://clickhouse.com/clickhouse#getting_started
3. Add your config using template and renaming file "config.json"
4. Make sure you have Python 3 (Code is specifically known to support 3.9), and you run `pip install -r requirements.txt` in the project directory

## DB Setup For WSL
Here's some general setup instructions for DB Setup on WSL:
1. Install MySQL Server. Here is one tutorial for how to do so: https://pen-y-fan.github.io/2021/08/08/How-to-install-MySQL-on-WSL-2-Ubuntu/
2. Make sure you run `sudo service mysqld start`. Then, navigate to this project's directory. Then, if you are not able to run `mysql` normally, run `sudo mysql -h 127.0.0.1 -P 3306 -u root` and enter your root password.
3. Once inside the MySQL shell, run `source create_database.sql`
4. Install Clickhouse. These steps worked for me: https://clickhouse.com/docs/en/install/#install-from-deb-packages. If you set a password, make sure to assign `password_clickhouse` in the config file.
5. Run `clickhouse-client --password <your_password_here> --queries-file create_database_clickhouse.sql`

## Script Descriptions
1. Use `add_rows` to add data to the MySQL database
2. Use `migrate_to_clickhouse` to migrate data from MySQL to Clickhouse
3. TODO describe others once complete