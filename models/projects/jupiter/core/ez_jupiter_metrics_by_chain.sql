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
    date
    , 'solana' as chain
    , 'jupiter' as artemis_id

    -- Market Metrics
    , price
    , market_cap
    , fdmc
    , token_volume

    -- Usage Metrics
    , aggregator_volume
    , perp_volume

    , aggregator_txns
    , perp_txns
    , txns

    , aggregator_dau
    , perp_dau
    , dau

    -- TVL Metrics
    , lst_tvl
    , perp_tvl
    , tvl
    
    , aggregator_fees
    , perp_fees
    , fees
    , service_fee_allocation
    , treasury_fee_allocation
    , aggregator_revenue

    -- Revenue Metrics
    , revenue
    , buybacks

    -- Other Metrics
    , token_turnover_circulating
    , token_turnover_fdv
from {{ref("ez_jupiter_metrics")}}