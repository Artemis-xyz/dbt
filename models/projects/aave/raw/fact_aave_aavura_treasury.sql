{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_aavura_treasury",
    )
}}


WITH 
tokens as (
    SELECT LOWER(address) AS address
    FROM (
        VALUES
        ('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'),
        ('0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'),
        ('0xdAC17F958D2ee523a2206206994597C13D831ec7'),
        ('0x5aFE3855358E112B5647B952709E6165e1c1eEEe'),
        ('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        ('0x6B175474E89094C44Da98b954EedeAC495271d0F')
    ) AS addresses(address)
)
, base AS (
    select
        to_address,
        from_address,
        contract_address,
        block_timestamp::date as date,
        amount_precise,
        min(block_timestamp::date) OVER() as min_date
    FROM ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) in (select address from tokens)
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
        contract_address,
        SUM(CASE WHEN to_address = lower('0x89C51828427F70D77875C6747759fB17Ba10Ceb0') THEN amount_precise ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address = lower('0x89C51828427F70D77875C6747759fB17Ba10Ceb0') THEN amount_precise ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1, 2
    ORDER BY 1 DESC
)
, prices as (
    select
        hour::date as date
        , token_address
        , avg(price) as price
    from ethereum_flipside.price.ez_prices_hourly
    where token_address in (select address from tokens)
    group by 1, 2
)

SELECT
    dr.date AS date
    , 'ethereum' as chain
    , contract_address as token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (partition by contract_address ORDER BY dr.date) as amount_nominal
    , amount_nominal * p.price as amount_usd
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p 
    on p.date = dr.date 
    and lower(p.token_address) = lower(f.contract_address)
ORDER BY date DESC