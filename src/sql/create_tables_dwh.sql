drop table if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.global_metrics;
create table KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.global_metrics (
    date_update timestamp not null,
	currency_from int not null,
	amount_total numeric(14,2) not null,
	cnt_transactions int not null,
    avg_transactions_per_account numeric(14,2) not null,
    cnt_accounts_make_transactions int not null
);