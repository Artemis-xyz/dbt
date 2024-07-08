{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_treasury_native_usd",
    )
}}


WITH base AS (
    select
        to_address,
        from_address,
        TO_DATE(block_timestamp) as date,
        amount_precise,
        MIN(TO_DATE(block_timestamp)) OVER() as min_date
    FROM ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x514910771AF9Ca656af840dff83E8264EcF986CA')
),  date_range AS (
    SELECT *
        FROM (
            SELECT
                min_date + SEQ4() AS date
            FROM base
        )
    WHERE date <= TO_DATE(SYSDATE())
),
address_cte as (
    {{chainlink_non_circulating_supply_addresses() }}
),
flows as (
    SELECT
        date,
        SUM(CASE WHEN to_address IN (SELECT address FROM address_cte) THEN amount_precise ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address IN (SELECT address FROM address_cte) THEN amount_precise ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1
    ORDER BY 1 DESC
),
prices as(
    SELECT
        date(hour) as date,
        AVG(price) as price
    FROM ethereum_flipside.price.ez_prices_hourly
    where lower(token_address) = lower('0x514910771AF9Ca656af840dff83E8264EcF986CA')
    group by 1
)
SELECT
    dr.date AS date,
    SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (ORDER BY dr.date) as treasury_link,
    treasury_link * p.price as treasury_usd,
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p on p.date = dr.date
ORDER BY date DESC