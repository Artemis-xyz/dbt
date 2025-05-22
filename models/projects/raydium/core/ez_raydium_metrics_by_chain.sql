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
    , buyback_cash_flow
    , treasury_cash_flow
    , service_cash_flow
    , buybacks
    , buyback_native
    , trading_volume
FROM
    {{ ref("ez_raydium_metrics") }}
