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
    , t2.token_symbol AS symbol
    , coingecko_id
    , shifted_token_price_usd AS price
    , shifted_token_market_cap AS market_cap
    , shifted_token_h24_volume_usd AS h24_volume
    , t2.token_max_supply AS max_supply
    , t2.token_total_supply AS total_supply
    , t2.token_ath AS ath
    , t2.token_ath_change_percentage AS ath_change_percentage
    , t2.token_ath_date AS ath_date
    , t2.token_atl AS atl
    , t2.token_atl_change_percentage AS atl_change_percentage
    , t2.token_atl_date AS atl_date
    , t2.fdv_supply * shifted_token_price_usd AS fdmc
FROM
    {{ source("PC_DBT_DB_UPSTREAM", "fact_coingecko_token_date_adjusted_gold") }}
        AS t1
INNER JOIN (
    SELECT
        token_id
        , token_symbol
        , token_max_supply
        , token_total_supply
        , token_ath
        , token_ath_change_percentage
        , token_ath_date
        , token_atl
        , token_atl_change_percentage
        , token_atl_date
        , coalesce(token_max_supply, token_total_supply) AS fdv_supply
    FROM
        {{ source("PC_DBT_DB_UPSTREAM", "fact_coingecko_token_realtime_data") }}
) AS t2 ON t1.coingecko_id = t2.token_id
