-- Удаляем данные для идемподентности
DELETE FROM
    L4244YANDEXRU__STAGING.currencies
WHERE
    date_update :: date = '{date}';

-- Загружаем данные
COPY L4244YANDEXRU__STAGING.currencies
                        FROM
                        LOCAL 'currencies-{date}.csv' DELIMITER ','