{{
    config(
        materialized="table",
        snowflake_warehouse="DYDX",
        database="dydx_v4",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_dydx_v4_trading_volume") }}
    ),
    fees_data as (
        select date, maker_fees, taker_fees, fees
        from {{ ref("fact_dydx_v4_fees") }}
    ),
    chain_data as (
        select date, trading_fees, txn_fees
        from {{ ref("fact_dydx_v4_txn_and_trading_fees") }}
    ),
    unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_dydx_v4_unique_traders") }}
    )

select 
    unique_traders_data.date as date
    , 'dydx_v4' as app
    , 'DeFi' as category
    , 'dydx_v4' as chain
    , trading_volume
    , unique_traders
    , maker_fees
    , taker_fees
    , fees
    , trading_fees -- Trading fees is maker_fees+taker_fees
    , txn_fees -- chain transaction fees (not really significant)
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    , trading_fees + txn_fees as gross_protocol_revenue
    , case when unique_traders_data.date > '2025-03-25' then gross_protocol_revenue * 0.25 else 0 end as buybacks
from trading_volume_data
left join fees_data on trading_volume_data.date = fees_data.date
left join chain_data on trading_volume_data.date = chain_data.date
left join unique_traders_data on trading_volume_data.date = unique_traders_data.date
where unique_traders_data.date < to_date(sysdate())
