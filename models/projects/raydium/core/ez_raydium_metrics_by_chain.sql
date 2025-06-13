{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
        database="raydium",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

SELECT
    date
    , 'solana' as chain
    , spot_dau
    , spot_txns
    , spot_volume
    , tvl
    , spot_fees
    , pool_creation_fees
    , ecosystem_revenue
    , buyback_fee_allocation
    , treasury_fee_allocation
    , service_fee_allocation
    , buybacks
    , buyback_native
    , trading_volume
FROM
    {{ ref("ez_raydium_metrics") }}
