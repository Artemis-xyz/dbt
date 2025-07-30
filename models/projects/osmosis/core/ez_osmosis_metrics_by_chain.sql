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
    , fees / txns as chain_avg_txn_fee
    , chain_spot_volume -- Osmosis is both a DEX and a chain
    , spot_volume
    , tvl

    -- Fee Metrics
    , chain_fees
    , spot_fees
    , fees as fees
    , lp_fee_allocation
    , validator_fee_allocation

    -- Financial Metrics
    , revenue

    -- Crypto Metrics
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
FROM {{ ref("ez_osmosis_metrics") }}