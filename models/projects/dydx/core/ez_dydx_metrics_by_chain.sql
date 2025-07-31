{{
    config(
        materialized="table",
        snowflake_warehouse="DYDX",
        database="dydx",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data_v4 as (
        select date, trading_volume
        from {{ ref("fact_dydx_v4_trading_volume") }}
    )
    , fees_data_v4 as (
        select date, maker_fees, taker_fees, fees
        from {{ ref("fact_dydx_v4_fees") }}
    )
    , chain_data_v4 as (
        select date, maker_fees, maker_rebates, txn_fees
        from {{ ref("fact_dydx_v4_txn_fees") }}
    )
    , trading_fees_v4 as (
        select date, total_fees
        from {{ ref("fact_dydx_v4_trading_fees") }}
    )
    , unique_traders_data_v4 as (
        select date, unique_traders
        from {{ ref("fact_dydx_v4_unique_traders") }}
    )
    , trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_dydx_trading_volume") }}
        where market_pair is null
    )
    , unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_dydx_unique_traders") }}
    )

select 
    unique_traders_data.date as date
    , 'dydx' as artemis_id
    , 'starkware' as chain

    --Usage Data
    , unique_traders_data.unique_traders as perp_dau
    , unique_traders_data.unique_traders as dau
    , trading_volume_data.trading_volume as perp_volume
    
     --Fee Data
    , NULL as trading_fees
    , NULL as chain_fees
    , chain_fees + trading_fees as fees

from unique_traders_data
left join trading_volume_data on unique_traders_data.date = trading_volume_data.date
union all 
select 
    unique_traders_data_v4.date as date
    , 'dydx' as artemis_id
    , 'dydx' as chain

    --Usage Data
    , unique_traders as perp_dau
    , unique_traders as dau
    , trading_volume as perp_volume

    --Fee Data
    , txn_fees
    , total_fees as trading_fees
    , chain_fees + trading_fees as fees

from trading_volume_data_v4
left join fees_data_v4 on trading_volume_data_v4.date = fees_data_v4.date
left join chain_data_v4 on trading_volume_data_v4.date = chain_data_v4.date
left join unique_traders_data_v4 on trading_volume_data_v4.date = unique_traders_data_v4.date
left join trading_fees_v4 on trading_volume_data_v4.date = trading_fees_v4.date
where unique_traders_data_v4.date < to_date(sysdate())
