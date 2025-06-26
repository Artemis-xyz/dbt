{{
    config(
        materialized="table",
        snowflake_warehouse="STELLAR",
        database="stellar",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}
WITH fundamental_data AS (
    SELECT
        * EXCLUDE date,
        TO_TIMESTAMP_NTZ(date) AS date
    FROM {{ source('PROD_LANDING', 'ez_stellar_metrics_by_category_v2') }}
), prices as ({{ get_coingecko_price_with_latest("stellar") }})
, price_data as ({{ get_coingecko_metrics("stellar") }})
SELECT
    fundamental_data.date
    , fundamental_data.chain
    , fundamental_data.category
    , fundamental_data.classic_txns AS txns
    , fundamental_data.soroban_txns AS soroban_txns
    , fundamental_data.daily_fees as gas
    , fundamental_data.daily_fees * price as gas_usd
    , fundamental_data.operations as operations
    , fundamental_data.dau as dau
    -- Standardized Metrics
    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , fundamental_data.returning_users as returning_users
    , fundamental_data.new_users as new_users
    -- Cash Flow Metrics
    , gas as ecosystem_revenue_native
    , gas_usd as ecosystem_revenue
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
    , null AS contract_count
    , null AS real_users
FROM fundamental_data
LEFT JOIN prices USING(date)
LEFT JOIN price_data USING(date)
