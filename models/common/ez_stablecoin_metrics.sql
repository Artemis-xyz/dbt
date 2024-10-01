
{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}

SELECT
    date_granularity as date
    , chain
    , symbol
    , stablecoin_dau
    , stablecoin_transfer_volume
    , stablecoin_daily_txns
    , stablecoin_avg_txn_value
    , artemis_stablecoin_dau
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_daily_txns
    , artemis_stablecoin_avg_txn_value
    , p2p_stablecoin_dau
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , p2p_stablecoin_avg_txn_value
    , stablecoin_supply
    , p2p_stablecoin_supply
FROM {{ref("agg_daily_stablecoin_breakdown_symbol_chain")}}
