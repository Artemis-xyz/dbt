{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}

SELECT
    date
    , coingecko_id
    , shifted_token_price_usd AS price
    , shifted_token_market_cap AS market_cap
    , shifted_token_h24_volume_usd AS h24_volume
FROM
    {{ source("PC_DBT_DB_UPSTREAM", "fact_coingecko_token_date_adjusted_gold") }}
