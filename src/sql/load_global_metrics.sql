insert into KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.global_metrics 
(date_update, currency_from, amount_total, cnt_transactions,
avg_transactions_per_account, cnt_accounts_make_transactions)
(with temp as (select 
date_trunc('day', date_update) as date,
currency_code,
currency_code_with,
currency_with_div
from KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies
where currency_code_with = 420)
select 
date_trunc('day', transaction_dt) as date_update,
tr.currency_code as currency_from,
(case when currency_with_div is not null then sum(amount)*currency_with_div else sum(amount) end) as amount_total,
count(*) as cnt_transactions,
count(*)/count(distinct account_number_from) as avg_transactions_per_account,
count(distinct account_number_from) as cnt_accounts_make_transactions 
from KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions as tr
left join temp on (date_trunc('day', tr.transaction_dt) = temp.date and tr.currency_code = temp.currency_code)
where tr.account_number_from >= 0 and tr.status ='done' and 
date_trunc('day', tr.transaction_dt) < :current_update_dt
and date_trunc('day', tr.transaction_dt) >= :latest_update_dt
group by date_update, currency_from, currency_with_div 
order by date_update, currency_from);