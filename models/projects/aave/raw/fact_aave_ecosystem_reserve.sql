{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_ecosystem_reserve",
    )
}}


WITH 
base AS (
    select
        to_address,
        from_address,
        block_timestamp::date as date,
        amount_precise,
        min(block_timestamp::date) OVER() as min_date
    FROM ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
)
,  date_range AS (
    SELECT *
        FROM (
            SELECT
                min_date + SEQ4() AS date
            FROM base
        )
    WHERE date <= TO_DATE(SYSDATE())
)
, flows as (
    SELECT
        date,
        SUM(CASE WHEN to_address = lower('0x25F2226B597E8F9514B3F68F00f494cF4f286491') THEN amount_precise ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address = lower('0x25F2226B597E8F9514B3F68F00f494cF4f286491') THEN amount_precise ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1
    ORDER BY 1 DESC
)
, prices as ({{get_coingecko_price_with_latest('aave')}})

SELECT
    dr.date AS date
    , 'ethereum' as chain
    , '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9' as token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (ORDER BY dr.date) as amount_nominal
    , amount_nominal * p.price as amount_usd
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p on p.date = dr.date
ORDER BY date DESC