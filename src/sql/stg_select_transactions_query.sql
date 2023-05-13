SELECT
    *
FROM
    public.transactions
where
    transaction_dt :: date = '{date}'