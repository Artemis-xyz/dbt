{{
    config(
        materialized="table",
        snowflake_warehouse="OSMOSIS",
        database="osmosis",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}
SELECT
    date
    , 'osmosis' as artemis_id
    , 'osmosis' as chain
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , txns as txns
    , dau as chain_dau
    , dau as dau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_spot_volume -- Osmosis is both a DEX and a chain
    , dex_volumes as spot_volume
    , tvl

    -- Fee Metrics
    , gas_usd as chain_fees
    , trading_fees as spot_fees
    , fees as fees
    , trading_fees as service_fee_allocation
    , gas_usd as validator_fee_allocation

    -- Financial Metrics
    , revenue

    -- Crypto Metrics
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
FROM {{ ref("ez_osmosis_metrics") }}