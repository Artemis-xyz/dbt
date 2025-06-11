{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_eth_prices"
    )
}}

SELECT *
FROM {{ ref('fact_token_prices') }}
WHERE token = 'ETH'