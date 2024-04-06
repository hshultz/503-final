
INSERT INTO exchange_rates (base_currency_code, target_currency_code, rate_value, rate_date)
SELECT 
  raw_data.base,
  jsonb_object_keys(raw_data.rates) AS currency,
  (raw_data.rates ->> jsonb_object_keys(raw_data.rates))::NUMERIC AS rate_value,
  to_timestamp(raw_data.timestamp) AS rate_date
FROM (
  SELECT 
    (raw_json ->> 'base')::CHAR(3) AS base,
    (raw_json ->> 'date')::DATE AS date,
    (raw_json -> 'rates')::jsonb AS rates,
    (raw_json ->> 'timestamp')::BIGINT AS timestamp
  FROM currencies
) AS raw_data;
DELETE FROM currencies;


INSERT INTO currency_updates (base_currency_code, update_date)
SELECT 
    raw_json->>'base' AS base_currency_code,
    (raw_json->>'date')::DATE AS update_date
FROM 
    currencies;
DELETE FROM currencies;


INSERT INTO currency_codes (code)
SELECT DISTINCT jsonb_object_keys(raw_data.rates)
FROM (
  SELECT (raw_json -> 'rates')::jsonb AS rates
  FROM currencies
) AS raw_data
ON CONFLICT (code) DO NOTHING;
DELETE FROM currencies;


INSERT INTO daily_exchange_rates (base_currency_id, target_currency_id, exchange_rate, rate_date)
SELECT 
    (SELECT currency_id FROM currency_codes WHERE code = raw_data.base) AS base_currency_id,
    (SELECT currency_id FROM currency_codes WHERE code = currency) AS target_currency_id,
    (raw_data.rates ->> currency)::DECIMAL(14, 6) AS exchange_rate,
    raw_data.date AS rate_date
FROM (
  SELECT 
    (raw_json ->> 'base')::CHAR(3) AS base,
    (raw_json -> 'rates')::jsonb AS rates,
    (raw_json ->> 'date')::DATE AS date
  FROM currencies
) AS raw_data, jsonb_each(raw_data.rates) AS rate_data(currency, value);
DELETE FROM currencies;



INSERT INTO historical_rates (exchange_rate_id, exchange_rate, recorded_at)
SELECT 
    exchange_rate_id,
    exchange_rate,
    (SELECT added_at FROM currencies WHERE id = raw_data.currency_id) AS recorded_at
FROM daily_exchange_rates
JOIN (
  SELECT id AS currency_id, (raw_json ->> 'base')::CHAR(3) AS base
  FROM currencies
) AS raw_data ON daily_exchange_rates.base_currency_id = (SELECT currency_id FROM currency_codes WHERE code = raw_data.base);
DELETE FROM currencies;


INSERT INTO exchange_rate_strength (base_currency_code, target_currency_code, inverse_rate, rate_date)
SELECT 
    base_currency_code,
    currency_code,
    (1 / rate_value::NUMERIC) AS inverse_rate, -- This inverts the exchange rate
    rate_date
FROM (
    SELECT 
        (raw_json->>'base')::CHAR(3) AS base_currency_code,
        jsonb_object_keys(raw_json->'rates') AS currency_code,
        (raw_json->'rates'->jsonb_object_keys(raw_json->'rates'))::NUMERIC AS rate_value,
        (raw_json->>'date')::DATE AS rate_date
    FROM 
        currencies
) sub_query
ORDER BY rate_date, inverse_rate;
DELETE FROM currencies;