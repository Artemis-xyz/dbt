{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date,
    'solana' as chain,
    aggregator_dau,
    aggregator_fees,
    aggregator_txns,
    aggregator_volume,
    buyback_fee_allocation,
    fdmc,
    trading_volume,
    txns,
    fees,
    market_cap,
    perp_dau,
    perp_fees,
    perp_txns,
    perp_volume,
    price,
    service_fee_allocation,
    token_turnover_circulating,
    token_turnover_fdv,
    token_volume,
    treasury_fee_allocation,
    unique_traders,
    aggregator_unique_traders,
    aggregator_revenue
from {{ref("ez_jupiter_metrics")}}