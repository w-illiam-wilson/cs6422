drop database if exists transactions;
create database if not exists transactions;
use transactions;

drop table if exists purchases;
create table purchases (
  time_purchase DATETIME(3) not null,
  customer_id integer not null,
  cost FLOAT not null,
  store_num integer not null,
  primary key (time_purchase)
) engine=innodb;

select * from purchases;