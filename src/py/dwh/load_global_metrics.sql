INSERT INTO KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.global_metrics (
	date_update
	,currency_from
	,amount_total
	,cnt_transactions
	,avg_transactions_per_account
	,cnt_accounts_make_transactions
	) (
	WITH TEMP AS (
		SELECT date_trunc('day', date_update) AS DATE
			,currency_code
			,currency_code_with
			,currency_with_div
		FROM KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies
		WHERE currency_code_with = 420
		) SELECT date_trunc('day', transaction_dt) AS date_update
	,tr.currency_code AS currency_from
	,(
		CASE 
			WHEN currency_with_div IS NOT NULL
				THEN sum(amount) * currency_with_div
			ELSE sum(amount)
			END
		) AS amount_total
	,count(*) AS cnt_transactions
	,count(*) / count(DISTINCT account_number_from) AS avg_transactions_per_account
	,count(DISTINCT account_number_from) AS cnt_accounts_make_transactions FROM KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions AS tr LEFT JOIN TEMP ON (
		date_trunc('day', tr.transaction_dt) = TEMP.DATE
		AND tr.currency_code = TEMP.currency_code
		) WHERE tr.account_number_from >= 0
	AND tr.STATUS = 'done'
	AND date_trunc('day', tr.transaction_dt) < :current_update_dt
	AND date_trunc('day', tr.transaction_dt) >= :latest_update_dt GROUP BY date_update
	,currency_from
	,currency_with_div ORDER BY date_update
	,currency_from
	);