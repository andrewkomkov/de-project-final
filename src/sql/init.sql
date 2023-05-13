-- Сначала удалим табличку (если она есть)
DROP TABLE IF EXISTS L4244YANDEXRU__STAGING.transactions CASCADE;

-- Затем создадим в нужной нам структуре
CREATE TABLE IF NOT EXISTS L4244YANDEXRU__STAGING.transactions (
    operation_id UUID,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code int NULL,
    country varchar(30) NULL,
    status varchar(30) NULL,
    transaction_type varchar(30) NULL,
    amount int NULL,
    transaction_dt timestamp NOT NULL
)
order by
    currency_code,
    transaction_type,
    country,
    status SEGMENTED BY HASH(transaction_dt, operation_id) all nodes PARTITION BY transaction_dt :: date
GROUP BY
    calendar_hierarchy_day(transaction_dt :: date, 3, 2);

-- Удаление currencies
DROP TABLE IF EXISTS L4244YANDEXRU__STAGING.currencies CASCADE;

-- Создание currencies
CREATE TABLE IF NOT EXISTS L4244YANDEXRU__STAGING.currencies (
    date_update timestamp NULL,
    currency_code int NULL,
    currency_code_with int NULL,
    currency_with_div numeric(5, 3) NULL
)
order by
    currency_code UNSEGMENTED ALL NODES PARTITION BY date_update :: date;

-- Удаление витрины
DROP TABLE IF EXISTS L4244YANDEXRU__DWH.global_metrics CASCADE;

-- Создание витрины
CREATE TABLE IF NOT EXISTS L4244YANDEXRU__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from CHAR(3) NOT NULL,
    amount_total NUMERIC(18, 2) NOT NULL,
    cnt_transactions INT NOT NULL,
    avg_transactions_per_account NUMERIC(18, 2) NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL
)
order by
    currency_from SEGMENTED BY HASH(date_update, currency_from) all nodes PARTITION BY date_update;

-- Удаление проекции если она есть проекции для таблицы transactions
DROP PROJECTION IF EXISTS L4244YANDEXRU__STAGING.transactions_by_date;

-- Создание простой проекции по датам для таблицы transactions
CREATE PROJECTION IF NOT EXISTS L4244YANDEXRU__STAGING.transactions_by_date
(
    transaction_dt,
    currency_code,
    transaction_type,
    country,
    status,
    amount
)
AS
SELECT 
    transaction_dt :: date,
    currency_code,
    transaction_type,
    country,
    status,
    SUM(amount) AS amount
FROM 
    L4244YANDEXRU__STAGING.transactions
GROUP BY 
    transaction_dt :: date, currency_code, transaction_type, country, status;

-- Удаление проекции если она есть проекции для витрины global_metrics
DROP PROJECTION IF EXISTS L4244YANDEXRU__DWH.global_metrics_dates;
-- Создание простой проекции по датам для витрины global_metrics
CREATE PROJECTION IF NOT EXISTS L4244YANDEXRU__DWH.global_metrics_dates as
   SELECT
	  date_update
	, currency_from
	, sum(amount_total) AS "amount_total"
FROM
	L4244YANDEXRU__DWH.global_metrics
GROUP BY date_update,currency_from