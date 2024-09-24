{{
    config(
        materialized="table",
        snowflake_warehouse="outerlands"
    )
}}

SELECT * FROM VALUES
    ( 'bitcoin', 'bitcoin', 'BTC', '2013-04-30', null),
    ( 'ethereum', 'ethereum', 'ETH', '2013-04-30', null),
    ( 'solana', 'solana', 'SOL', '2021-01-01', '2023-01-01'),
    ('tron', 'tron', 'TRX', '2023-01-01', null)
AS sigma_example(artemis_id, coingecko_id, symbol, added_date, deleted_date)