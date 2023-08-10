drop projection if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions_date;
drop table if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions;
create table KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions (
	operation_id varchar(60) primary key,
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp
)
order by operation_id, transaction_dt
segmented by hash(operation_id, transaction_dt) all nodes;
create projection KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions_date
(
 transaction_dt
)
as
select transactions.transaction_dt 
from KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions
order by transactions.transaction_dt
segmented by hash(transactions.transaction_dt) all nodes;

drop projection if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies_date;
drop table if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies; 
create table KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies ( 
    date_update timestamp, 
	currency_code int, 
	currency_code_with int, 
	currency_with_div numeric(5, 3) 
)
order by date_update
segmented by hash(date_update) all nodes; 
create projection KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies_date
(
 date_update
)
as
select currencies.date_update
from KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies
order by currencies.date_update
segmented by hash(currencies.date_update) all nodes;

drop table if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.time_settings;
create table KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.time_settings
(
    database varchar(80) primary key,
    update_ts timestamp not null
);