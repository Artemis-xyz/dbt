{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="fact_uniswap_treasury_usd",
    )
}}

SELECT date, sum(usd_balance) as treasury_usd
FROM {{ ref('fact_uniswap_treasury_by_token') }}
GROUP BY 1