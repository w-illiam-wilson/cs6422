drop database if exists transactions;
create database if not exists transactions;
use transactions;
drop table if exists dirty_table;
create table dirty_table (
  stockname VARCHAR(30) NOT NULL,
  date DATETIME NOT NULL,
  updating INT NOT NULL,
  transaction_type INT NOT NULL, 
  PRIMARY KEY(stockname, date, updating)
) engine=innodb;
