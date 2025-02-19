{{
    config(
        materialized="table",
        snowflake_warehouse="PERPETUAL_PROTOCOL",
        database="perpetual_protocol",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_perpetual_protocol_trading_volume") }}
        where chain is not null
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_perpetual_protocol_unique_traders") }}
        where chain is not null
    ),
    fees_data as (
        select date, fees, chain
        from {{ ref("fact_perpetual_protocol_fees") }}
        where chain is not null
    ),
    tvl_data as (
        select date, tvl, chain
        from {{ ref("fact_perpetual_protocol_tvl") }}
        where chain is not null
    )
select
    date as date,
    'perpetual_protocol' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders,
    fees,
    fees * 0.2 as revenue,
    tvl,
    {{ daily_pct_change('tvl') }} as tvl_growth
from unique_traders_data
left join trading_volume_data using(date, chain)
left join fees_data using(date, chain)
left join tvl_data using(date, chain)
where date < to_date(sysdate())