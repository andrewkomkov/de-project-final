-- Удаляем данные для идемподентности
DELETE FROM
    L4244YANDEXRU__STAGING.transactions
WHERE
    transaction_dt :: date = '{date}';

-- Загружаем данные
COPY L4244YANDEXRU__STAGING.transactions
                        FROM
                        LOCAL 'transactions-{date}.csv' DELIMITER ','