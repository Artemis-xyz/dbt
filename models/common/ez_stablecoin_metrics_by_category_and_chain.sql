{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        alias="ez_stablecoin_metrics_by_category_chain_symbol",
        materialized='view'
    )
}}

SELECT
    DATE_GRANULARITY
    , CHAIN
    , SYMBOL
    , CATEGORY
    , STABLECOIN_DAU
    , STABLECOIN_TRANSFER_VOLUME
    , STABLECOIN_DAILY_TXNS
    , STABLECOIN_AVG_TXN_VALUE
    , ARTEMIS_STABLECOIN_DAU
    , ARTEMIS_STABLECOIN_TRANSFER_VOLUME
    , ARTEMIS_STABLECOIN_DAILY_TXNS
    , ARTEMIS_STABLECOIN_AVG_TXN_VALUE
    , P2P_STABLECOIN_DAU
    , P2P_STABLECOIN_TRANSFER_VOLUME
    , P2P_STABLECOIN_DAILY_TXNS
    , P2P_STABLECOIN_AVG_TXN_VALUE
    , STABLECOIN_SUPPLY
    , P2P_STABLECOIN_SUPPLY
FROM {{ref("agg_daily_stablecoin_breakdown_category_symbol_chain")}}