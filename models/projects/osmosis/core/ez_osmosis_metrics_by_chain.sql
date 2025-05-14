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
    , 'osmosis' as chain
    , txns
    , dau
    , gas_usd
    , trading_fees
    , fees
    , fees / txns as avg_txn_fee
    , revenue
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_spot_volume -- Osmosis is both a DEX and a chain
    , dex_volumes as spot_volume
    -- Cash Flow Metrics
    , gas_usd as chain_fees
    , trading_fees as spot_fees
    , fees as gross_protocol_revenue
    , trading_fees as service_cash_flow
    , gas_usd as validator_cash_flow
    -- Crypto Metrics
    , tvl
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
FROM {{ ref("ez_osmosis_metrics") }}