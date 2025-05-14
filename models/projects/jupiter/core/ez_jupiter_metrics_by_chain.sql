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
    buyback_cash_flow,
    fdmc,
    gross_protocol_revenue,
    market_cap,
    perp_dau,
    perp_fees,
    perp_txns,
    perp_volume,
    price,
    service_cash_flow,
    token_turnover_circulating,
    token_turnover_fdv,
    token_volume,
    treasury_cash_flow
from {{ref("ez_jupiter_metrics")}}